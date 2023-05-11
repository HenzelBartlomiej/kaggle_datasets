# kaggle_datasets
analysis of kaggle datasets 2023.05


this module uses kaggle api to get the dataset from
https://www.kaggle.com/datasets/

### !!! it requires kaggle api token - which needs to be created manually
https://www.kaggle.com/docs/api

In this exercise it is stored in kaggle.json in the Databricks Workspace dbfs : dbfs:/FileStore/kaggle.json - as we don't have access to the Key Vault
In real life project secrets should be stored in eg. Azure Key Vault

This module contains 3 functions:
  function to authenticate kaggle api
### kaggle_auth(KAGGLE_USERNAME, KAGGLE_KEY)

  function to read csv file saved in a driver vm
### read_csv(path)

  function to 
    1 upload from kaggle, 
    2 unzip it 
    3 read csv file saved in a driver vm - in real life project data would be stored in some cloud storage account
    4 transform input data
    5 save the data as parquet in a target destination
### transform_and_write(KAGGLE_FILE_PATH, KAGGLE_FILE_NAME, TARGET_PATH)

Code is run from kaggle_ex_notebook.py which need to be imported to Databricks workspace
