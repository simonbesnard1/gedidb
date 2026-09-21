# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import pandas as pd
import pytest
from datetime import datetime
from shapely.geometry import box
import geopandas as gpd

# Adjust these imports to your actual module structure
from gedidb.downloader.data_downloader import CMRDataDownloader
from gedidb.downloader.data_downloader import GranuleQuery


@pytest.fixture
def geom():
    # A small bbox over Europe (doesn't matter; we mock CMR)
    g = gpd.GeoSeries([box(10, 50, 11, 51)], crs="EPSG:4326")
    return g


def _df(ids, product_value):
    # Minimal DataFrame returned by GranuleQuery.query_granules()
    # Required columns in your code: id, url, start_time, size
    return pd.DataFrame(
        {
            "id": ids,
            "url": [f"https://example/{product_value}/{i}" for i in ids],
            "start_time": [datetime(2020, 1, 1)] * len(ids),
            "size": ["1.0"] * len(ids),  # strings cast to float in code
        }
    )


def test_cmr_download_happy_path(monkeypatch, geom):
    """
    All required products present for the same granule IDs → filtered dict returned.
    """

    # Mock GediProduct to be the real enum (level2A, level2B, level4A, level4C)
    # Monkeypatch GranuleQuery.query_granules per product
    def fake_init(self, product, geom_, start, end, info):
        self.product = product

    def fake_query(self):
        pv = self.product.value
        # Two granules, both have all products
        return _df(["G1", "G2"], pv)

    monkeypatch.setattr(GranuleQuery, "__init__", fake_init, raising=False)
    monkeypatch.setattr(GranuleQuery, "query_granules", fake_query, raising=False)

    dl = CMRDataDownloader(
        geom=geom, start_date=datetime(2020, 1, 1), end_date=datetime(2020, 1, 2)
    )
    out = dl.download()
    # Both granules should remain; each with 4 product tuples
    assert set(out.keys()) == {"G1", "G2"}
    for v in out.values():
        assert len(v) == 4
        # (url, product_value, start_time)
        assert set(p for _, p, _, _ in v) == {
            "level2A",
            "level2B",
            "level4A",
            "level4C",
        }


def test_cmr_download_filters_missing_products(monkeypatch, geom):
    """
    If some products are missing for a granule, it should be filtered out.
    """

    def fake_init(self, product, geom_, start, end, info):
        self.product = product

    def fake_query(self):
        pv = self.product.value
        # Only G1 has all products; G2 misses level4C (we skip returning it for that product).
        if pv == "level4C":
            return _df(["G1"], pv)  # G2 absent for level4C
        return _df(["G1", "G2"], pv)

    monkeypatch.setattr(GranuleQuery, "__init__", fake_init, raising=False)
    monkeypatch.setattr(GranuleQuery, "query_granules", fake_query, raising=False)

    dl = CMRDataDownloader(geom=geom)
    out = dl.download()
    assert list(out.keys()) == ["G1"]  # G2 filtered out


def test_cmr_download_none_found_raises(monkeypatch, geom):
    """
    If no granules at all, raise ValueError with helpful message.
    """

    def fake_init(self, product, geom_, start, end, info):
        pass

    def fake_query(self):
        return pd.DataFrame(columns=["id", "url", "start_time", "size"])

    monkeypatch.setattr(GranuleQuery, "__init__", fake_init, raising=False)
    monkeypatch.setattr(GranuleQuery, "query_granules", fake_query, raising=False)

    dl = CMRDataDownloader(
        geom=geom, start_date=datetime(2021, 1, 1), end_date=datetime(2021, 1, 2)
    )
    with pytest.raises(ValueError) as e:
        dl.download()
    # Helpful content in message:
    assert "No GEDI granules found" in str(e.value)
    assert "Geometry bounds=" in str(e.value)


def test_ambiguous_product_sources_raise(geom):
    downloader = CMRDataDownloader(geom=geom)
    with pytest.raises(ValueError, match="Ambiguous sources"):
        downloader._filter_granules_with_all_products(
            {
                "G1": [
                    ("https://example/first.h5", "level2A", None),
                    ("https://example/second.h5", "level2A", None),
                ]
            }
        )


def test_required_products_can_select_l2a_only(geom):
    from gedidb.utils.constants import GediProduct

    downloader = CMRDataDownloader(geom=geom, products=[GediProduct.L2A])
    assert list(
        downloader._filter_granules_with_all_products(
            {
                "G1": [
                    ("https://example/l2a.h5", "level2A", None),
                ]
            }
        )
    ) == ["G1"]


def test_cmr_selects_data_link_not_first_link():
    item = {
        "links": [
            {"href": "https://example/documentation.html"},
            {"href": "https://example/data.h5?token=example"},
        ]
    }
    assert GranuleQuery._download_link(item) == "https://example/data.h5?token=example"
