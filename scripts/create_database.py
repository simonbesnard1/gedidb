from gedidb.core.gediprocessor import GEDIGranuleProcessor


#%% Initiate database builder
config_files_ = {"data_info": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/data_config.yml', 
                "database_schema": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/database_scheme.yaml'}
database_builder = GEDIGranuleProcessor(config_files = config_files_)

#%% Process GEDI data
database_builder.compute()
