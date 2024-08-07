from GEDItools.database.db_builder import GEDIGranuleProcessor


#%% Initiate database builder
database_builder = GEDIGranuleProcessor(database_config = '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/database_params.yml', 
                                        column_to_field_config = '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/column_to_field.yml')

 
#%% Process GEDI data
database_builder.process()
