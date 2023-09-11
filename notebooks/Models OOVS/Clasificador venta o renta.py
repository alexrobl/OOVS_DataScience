# Databricks notebook source
# MAGIC %md
# MAGIC Created on June - 2023 </br>
# MAGIC @author: Alexander Robles

# COMMAND ----------

import numpy as np
import pandas as pd
import os
import missingno as msno
from sklearn import preprocessing
from catboost import CatBoostClassifier, Pool
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, accuracy_score

# COMMAND ----------

df = pd.read_csv('/dbfs/FileStore/tables/procesado/BTA_INMO_IQR_FEBRERO_2023__2_.csv')

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt
 
from numpy import savetxt
 
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
 
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

db = load_diabetes()
X = db.data
y = db.target
X_train, X_test, y_train, y_test = train_test_split(X, y)

# COMMAND ----------

with mlflow.start_run():
  
  # Set the model parameters. 
  n_estimators = 100
  max_depth = 6
  max_features = 3
  
  # Create and train model.
  rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
  rf.fit(X_train, y_train)
  
  # Use the model to make predictions on the test dataset.
  predictions = rf.predict(X_test)
  
  # Log the model parameters used for this run.
  mlflow.log_param("num_trees", n_estimators)
  mlflow.log_param("maxdepth", max_depth)
  mlflow.log_param("max_feat", max_features)
  
  # Define a metric to use to evaluate the model.
  mse = mean_squared_error(y_test, predictions)
    
  # Log the value of the metric from this run.
  mlflow.log_metric("mse", mse)
  
  # Log the model created by this run.
  mlflow.sklearn.log_model(rf, "random-forest-model") 
  
  # Save the table of predicted values
  savetxt('predictions.csv', predictions, delimiter=',')
  
  # Log the saved table as an artifact
  mlflow.log_artifact("predictions.csv")
  
  # Convert the residuals to a pandas dataframe to take advantage of graphics capabilities
  df = pd.DataFrame(data = predictions - y_test)
  
  # Create a plot of residuals
  plt.plot(df)
  plt.xlabel("Observation")
  plt.ylabel("Residual")
  plt.title("Residuals")
 
  # Save the plot and log it as an artifact
  
  plt.savefig("residuals_plot.png")
  mlflow.log_artifact("residuals_plot.png") 
