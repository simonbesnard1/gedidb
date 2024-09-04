from gedidb.utils.gedi_metadata import GediMetaDataExtractor

#%% Extract metadata
url ='https://daac.ornl.gov/GEDI/guides/GEDI_L4A_AGB_Density.html'
output_file = "/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/metadata/gedi04_av002_config.yaml"

extractor = GediMetaDataExtractor(url, output_file)
extractor.run()
