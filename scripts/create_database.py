from gedidb.core.gediprocessor import GEDIGranuleProcessor


#%% Initiate database builder
config_files_ = {"database": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/database_structure.yml', 
                "schema": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/database_scheme.yaml',
                "column_to_field": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/column_to_field.yml',
                "quality_filter": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/quality_filters.yml',
                "field_mapping": '/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/field_mapping.yml'}
database_builder = GEDIGranuleProcessor(config_files = config_files_)

#%% Process GEDI data
database_builder.compute()
