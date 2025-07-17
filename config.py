import configparser
from msgraph import GraphServiceClient
from azure.identity import DeviceCodeCredential




def email_config():
    config= configparser.ConfigParser()
    config.read('config.ini')


    client_id = config["PwaniFinance"]["client_id"]
    tenant_id = config["PwaniFinance"]["tenant_id"]
    client_secret=config['PwaniFinance']['client_secret']
    
    return client_id, tenant_id,client_secret




def oracle_config():
    config = configparser.ConfigParser()
    config.read('config.ini')

    dsn = config["OracleDsn"]["ORACLE_DSN"]
    username = config["OracleDsn"]["ORACLE_USERNAME"]
    password = config["OracleDsn"]["ORACLE_PASSWORD"]

    return dsn, username, password




