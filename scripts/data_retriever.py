from gedidb.providers.db_provider import DatabaseInfoRetriever
from gedidb.database.db_creation import DatabaseManager

#%%

receiver = DatabaseInfoRetriever(DatabaseManager('postgresql://glmadmin:SimonGFZ@mefe27:5434/glmdb'))

print(receiver.get_table_names())

print(receiver.get_table_schema('filtered_l2ab_l4ac_shots'))
