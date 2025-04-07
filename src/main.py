from config_reader import read_config
from data_ingestion import DataIngestion

data_inges=DataIngestion(read_config("config/config.yaml"))
data_inges.run()