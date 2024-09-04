from gedidb.core.gediprocessor import GEDIGranuleProcessor

#%% Initiate database builder
database_builder = GEDIGranuleProcessor(data_config_file = "./config_files/data_config.yml", sql_config_file='./config_files/db_scheme.sql')

#%% Process GEDI data
database_builder.compute()

