from pathlib import Path
from logger import get_logger
import yaml

logger=get_logger(__name__)

def read_config(config_path):
    
    config_path = Path(config_path)
    if not config_path.is_file():
        logger.error(f"Config file not found at {config_path}")
        raise FileNotFoundError(f"Config file not found at {config_path}")
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except yaml.YAMLError as e:
        logger.error(f"Error reading YAML file: {e}")
        raise e
    
if __name__=="__main__":
    config=read_config("config/config.yaml")
    print(config["data_ingestion"]["bucket_name"])
    