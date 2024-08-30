from sqlalchemy import text

from gedidb.core.gediprocessor import GEDIGranuleProcessor
from gedidb.database.db import DatabaseManager
from gedidb.providers.db_provider import DatabaseInfoRetriever

#%% Initiate database builder
config_files_ = {"data_info": '../config_files/data_config.yml',
                "database_schema": '../config_files/database_scheme.yml'}
database_builder = GEDIGranuleProcessor(config_files = config_files_)

#%% Process GEDI data

# DatabaseManager('postgresql://glmadmin:SimonGFZ@mefe27:5434/glmdb').execute_query("DROP TABLE filtered_l2ab_l4ac_shots")

database_builder.compute()


