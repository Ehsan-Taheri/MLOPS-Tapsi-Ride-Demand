import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error
from pathlib import Path
from logger import get_logger

logger= get_logger(__name__)

class ModelTraining:

    def __init__(self,config):
        self.model_training_config=config["model_training"]
        artifact_dir=Path(config["data_ingestion"]["artifact_dir"])
        self.processed_dir=artifact_dir/"processed"

    def load_processed_data(self):
        train_data=pd.read_csv(self.processed_dir/"train_data.csv")
        val_data=pd.read_csv(self.processed_dir/"val_data.csv")
        return train_data,val_data
    def build_model(self):
        n_stimators=self.model_training_config["n_estimators"]
        max_sample=self.model_training_config["max_sample"]
        n_jobs=self.model_training_config["n_jobs"]


        model=RandomForestRegressor(n_estimators=n_stimators,max_samples=max_sample,
            n_jobs=n_jobs,
            oob_score=root_mean_squared_error)
        return model
    def train_model(self,model,train_data):
        X_train=train_data.drop(columns=["demand"])
        y_train=train_data["demand"]
        model.fit(X_train,y_train)

    def evaluate_model(self,model,val_data):
        X_val=val_data.drop(columns=["demand"])
        y_val=val_data["demand"]
        y_pred=model.predict(X_val)
        rmse=root_mean_squared_error(y_val,y_pred)
        logger.info(f"Out-of-bag score: {model.oob_score_}")
        logger.info(f"Validation RMSE: {rmse}")


    def run(self):
        logger.info("Model training started")
        # Load the processed data
        train_data,val_data=self.load_processed_data()
        model=self.build_model()
        self.train_model(model,train_data)
        self.evaluate_model(model,val_data)
        logger.info("Model training completed successfully")
