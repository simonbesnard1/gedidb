from gedidb.downloader.authentication import EarthDataAuthenticator


#%% Authentification to EarthData 
authenticator = EarthDataAuthenticator(config_file='/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/earth_data_credentials.yml')
authenticator.authenticate()
