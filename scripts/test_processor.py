import gedidb as gdb

if __name__ == '__main__':

    #%% Initiate database builder
    database_builder = gdb.GEDIProcessor(data_config_file = "./config_files/data_config.yml", 
                                         sql_config_file='./config_files/db_scheme.sql')
    
    #%% Process GEDI data
    database_builder.compute()

