from config_reader import read_config
from data_ingestion import DataIngestion
from data_preprocessing import DataPreprocessing
from model_training import ModelTraining

data_inges=DataIngestion(read_config("config/config.yaml"))
data_inges.run()

data_preprocessing=DataPreprocessing(read_config("config/config.yaml"))
data_preprocessing.run()

model_training=ModelTraining(read_config("config/config.yaml"))
model_training.run()

