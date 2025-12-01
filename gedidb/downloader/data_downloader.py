# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import pathlib
from collections import defaultdict
from datetime import datetime
from typing import Optional, Tuple
import threading
from retry import retry

logger = logging.getLogger(__name__)

# Thread-local storage for safe Sessions
_thread_local = threading.local()

import geopandas as gpd
import requests
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    HTTPError,
    RequestException,
    Timeout,
)
from urllib3.exceptions import NewConnectionError
import h5py

from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct

# Configure logging
logger = logging.getLogger(__name__)


# Create a filter to suppress WARNING messages
class WarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno != logging.WARNING  # Exclude only WARNING logs


# Apply the filter
logger.addFilter(WarningFilter())


def _normalize_entry(t):
    """Accept (url, product, start_time) or (url, product, start_time, size_mb)."""
    if len(t) == 4:
        url, product, start_time, size_mb = t
        return (
            url,
            product,
            start_time,
            float(size_mb) if size_mb is not None else 0.0,
        )
    elif len(t) == 3:
        url, product, start_time = t
        return (url, product, start_time, 0.0)
    else:
        raise ValueError(f"Unexpected granule tuple shape: {t!r}")


def _get_session() -> requests.Session:
    """Return a per-thread requests.Session (thread-safe)."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": "gedidb/1.0"})
        _thread_local.session = s
    return _thread_local.session


class GEDIDownloader:
    """
    Base class for GEDI data downloaders.
    """

    def _download(self, *args, **kwargs):
        """
        Abstract method that must be implemented by subclasses.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")


class CMRDataDownloader(GEDIDownloader):
    """
    Downloader for GEDI granules from NASA's CMR service.
    """

    def __init__(
        self,
        geom: gpd.GeoSeries,
        start_date: datetime = None,
        end_date: datetime = None,
        earth_data_info=None,
    ):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date
        self.earth_data_info = earth_data_info

    @retry(
        (
            ValueError,
            TypeError,
            HTTPError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            RequestException,
            NewConnectionError,
        ),
        tries=4,
        delay=2,
        backoff=2,
        logger=logger,
    )
    def download(self) -> dict:
        """
        Download granules across all GEDI products and ensure ID consistency.
        Returns: {granule_id: [(url, product, start_time, size_mb), ...]}
        """
        cmr_dict = defaultdict(list)
        per_product_counts = {}
        per_product_sizes_mb = {}

        # 1) Query per product and stage everything (include size for post-intersection sum)
        for product in GediProduct:
            try:
                granule_query = GranuleQuery(
                    product,
                    self.geom,
                    self.start_date,
                    self.end_date,
                    self.earth_data_info,
                )
                granules = granule_query.query_granules()

                if granules.empty:
                    logger.warning(f"No granules found for product {product.value}.")
                    per_product_counts[product.value] = 0
                    per_product_sizes_mb[product.value] = 0.0
                    continue

                per_product_counts[product.value] = len(granules)
                per_product_sizes_mb[product.value] = float(
                    granules["size"].astype(float).sum()
                )

                for _, row in granules.iterrows():
                    cmr_dict[row["id"]].append(
                        (
                            row["url"],
                            product.value,
                            row["start_time"],
                            float(row["size"]),
                        )
                    )

            except Exception as e:
                logger.error(f"Failed to download granules for {product.name}: {e}")
                per_product_counts.setdefault(product.value, 0)
                per_product_sizes_mb.setdefault(product.value, 0.0)
                continue

        if not cmr_dict:
            raise ValueError(
                "No GEDI granules found for the provided spatio-temporal request. "
                f"Geometry bounds={self.geom.total_bounds.tolist()}, "
                f"start_date={self.start_date}, end_date={self.end_date}"
            )

        # 2) Intersect to keep only granules that have all required products.
        filtered_cmr_dict = self._filter_granules_with_all_products(cmr_dict)
        if not filtered_cmr_dict:
            raise ValueError("No granules with all required products found.")

        # 3) True counts/sizes AFTER intersection.
        n_intersection = len(filtered_cmr_dict)
        total_size_mb = sum(
            sz for entries in filtered_cmr_dict.values() for _, _, _, sz in entries
        )

        # 4) Clear logging (and a sanity note)
        if per_product_counts:
            min_per_prod = min(per_product_counts.values())
            if n_intersection > min_per_prod:
                logger.warning(
                    "Intersection (%d) > min per-product count (%d) — check product set / inputs.",
                    n_intersection,
                    min_per_prod,
                )

        logger.info(
            "Intersection has %d granule IDs across %d products. "
            "Estimated download: %.2f GB (%.2f TB). ",
            n_intersection,
            len(GediProduct),
            total_size_mb / 1024,
            total_size_mb / 1_048_576,
        )

        return filtered_cmr_dict

    def _filter_granules_with_all_products(self, granules: dict) -> dict:
        """
        Keep only granule IDs that have all required products.
        Deduplicates multiple entries for the same (granule_id, product).
        Accepts tuples of len 3 or 4 and normalizes to len 4.
        """
        required_products = {p.value for p in GediProduct}
        filtered_granules = {}

        for granule_id, product_info in granules.items():
            # Normalize shapes & dedupe per product
            by_product = {}
            for t in product_info:
                url, product, start_time, size_mb = _normalize_entry(t)
                # keep first seen per product; change policy if you prefer newest/largest
                by_product.setdefault(product, (url, product, start_time, size_mb))

            # Check intersection condition
            if not required_products.issubset(by_product.keys()):
                continue

            # Keep only required products (ignore extras)
            filtered_granules[granule_id] = [by_product[p] for p in required_products]

        return filtered_granules


class H5FileDownloader:
    """
    Safe downloader for HDF5 files using thread-local Sessions
    and non-streaming chunked reads to avoid SSL segfaults in threaded mode.
    """

    def __init__(self, download_path: str = ".") -> None:
        self.download_path = pathlib.Path(download_path)

    @retry(
        (
            ValueError,
            TypeError,
            HTTPError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            RequestException,
            OSError,
        ),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger,
    )
    def download(
        self, granule_key: str, url: str, product
    ) -> Tuple[str, Tuple[str, Optional[str]]]:

        session = _get_session()

        granule_dir = self.download_path / granule_key
        granule_dir.mkdir(parents=True, exist_ok=True)

        product_name = getattr(product, "name", str(product))
        product_value = getattr(product, "value", str(product))

        final_path = granule_dir / f"{product_name}.h5"
        temp_path = granule_dir / f"{product_name}.h5.part"

        # -----------------------------
        # Fast path: valid existing file
        # -----------------------------
        if final_path.exists() and self._is_hdf5_valid(final_path):
            return granule_key, (product_value, str(final_path))

        # ensure stale files are removed
        final_path.unlink(missing_ok=True)

        # -----------------------------
        # Determine total size via HEAD-range
        # -----------------------------
        downloaded_size = temp_path.stat().st_size if temp_path.exists() else 0
        total_size: Optional[int] = None

        r = session.get(url, headers={"Range": "bytes=0-1"}, timeout=30)
        r.raise_for_status()

        cr = r.headers.get("Content-Range")
        if cr:
            try:
                total_size = int(cr.split("/")[-1])
            except Exception:
                total_size = None

        # -----------------------------
        # Prepare resume header
        # -----------------------------
        headers = {}
        if downloaded_size > 0:
            headers["Range"] = f"bytes={downloaded_size}-"

        # -----------------------------
        # Main download — no streaming
        # -----------------------------
        r = session.get(url, headers=headers, timeout=45, stream=False)
        r.raise_for_status()

        mode = "ab" if downloaded_size else "wb"
        with open(temp_path, mode) as f:
            data = r.content
            f.write(data)

        # -----------------------------
        # Validate size for complete files
        # -----------------------------
        final_size = temp_path.stat().st_size

        if total_size is not None and final_size != total_size:
            temp_path.unlink(missing_ok=True)
            raise ValueError(f"Size mismatch: expected {total_size}, got {final_size}")

        # -----------------------------
        # Promote part → final
        # -----------------------------
        temp_path.rename(final_path)

        # -----------------------------
        # Validate HDF5 integrity
        # -----------------------------
        if not self._is_hdf5_valid(final_path):
            final_path.unlink(missing_ok=True)
            raise ValueError("Invalid HDF5 file after download.")

        return granule_key, (product_value, str(final_path))

    def _is_hdf5_valid(self, file_path: pathlib.Path) -> bool:
        """Lightweight HDF5 validation."""
        try:
            return h5py.is_hdf5(file_path)
        except Exception:
            return False
