from gedidb.utils.gedi_metadata import GediMetaDataExtractor

#%% Extract metadata
url ='https://daac.ornl.gov/GEDI/guides/GEDI_L4C_WSCI.html'
#url = "https://lpdaac.usgs.gov/products/gedi02_av002/"
output_file = "/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/metadata/gedi04_av002_config.yaml"

extractor = GediMetaDataExtractor(url, output_file, data_type= 'L4C')
extractor.run()
