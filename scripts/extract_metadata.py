from gedidb.utils.gedi_metadata import GediMetaDataExtractor

#%% Extract metadata
url = "https://lpdaac.usgs.gov/products/gedi02_av002/"
output_file = "gedi02_av002_config.yaml"

extractor = GediMetaDataExtractor(url, output_file)
extractor.run()