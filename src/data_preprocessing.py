import pandas as pd
from pathlib import Path
from logger import get_logger


logger = get_logger(__name__)

class DataPreprocessing:
    def __init__(self,config):
        self.data_preprocessing_config = config["data_processing"]
        artifact_dir= Path(config["data_ingestion"]["artifact_dir"])
        self.raw_dir= artifact_dir / "raw"
        self.processed_dir= artifact_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def load_raw_data(self):
        
        train_data = pd.read_csv(self.raw_dir /"train.csv")
        val_data = pd.read_csv(self.raw_dir /"validation.csv")
        test_data = pd.read_csv(self.raw_dir /"test.csv")
        return train_data,val_data,test_data

    def preprocess_data(self,train_data,val_data,test_data):
        train_data = self._process_single_dataset(train_data)
        val_data = self._process_single_dataset(val_data)
        test_data = self._process_single_dataset(test_data)

        return train_data, val_data, test_data


   


    def save_data_to_CSV(self,processed_train_data,processed_val_data,processed_test_data):
        column_order = ["hour_of_day", "day", "row", "col", "demand"]

        processed_train_data = processed_train_data[column_order]
        processed_val_data = processed_val_data[column_order]
        processed_test_data = processed_test_data[column_order]

        # Save the processed data to CSV files
        processed_train_data.to_csv(self.processed_dir /"train_data.csv",index=False)
        processed_val_data.to_csv(self.processed_dir /"val_data.csv",index=False)
        processed_test_data.to_csv(self.processed_dir /"test_data.csv",index=False)



    def _process_single_dataset(self, data):
        """
        Transforms raw temporal data into meaningful features that capture
        daily and hourly patterns in taxi demand, making it easier for the
        model to learn time-based patterns.

        Parameters
        ----------
        data : pd.DataFrame
            Dataset to process

        Returns
        -------
        pd.DataFrame
            Dataset with engineered temporal features
        """
        data = data.sort_values(by=["time", "row", "col"]).reset_index(drop=True)

        data["time"] = data["time"] + self.data_preprocessing_config["shift"]

        data = data.assign(hour_of_day=data["time"] % 24, day=data["time"] // 24)

        data = data.drop(columns=["time"])

        return data



    def run(self):
        logger.info("Data preprocessing started")
        # Load the data
        train_data,val_data,test_data =self.load_raw_data()
        processed_train_data,processed_val_data,processed_test_data  =self.preprocess_data(train_data,val_data,test_data)
        self.save_data_to_CSV(processed_train_data,processed_val_data,processed_test_data)
        logger.info("Data preprocessing completed successfully")