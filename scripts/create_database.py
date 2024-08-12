from GEDItools.database.db_builder import GEDIGranuleProcessor


#%% Initiate database builder
database_builder = GEDIGranuleProcessor(database_config_file = '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/database_params.yml', 
                                        column_to_field_config_file = '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/column_to_field.yml',
                                        quality_config_file = '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/quality_filters.yml',
                                        field_mapping_config_file = '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/field_mapping.yml')

#%% Process GEDI data
database_builder.compute()
