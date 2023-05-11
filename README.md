# kaggle_datasets
analysis of kaggle datasets 2023.05


This module uses kaggle api to get the dataset from the https://www.kaggle.com/datasets/

### 1) !!! it requires kaggle api token - which needs to be created manually
https://www.kaggle.com/docs/api

Note: kaggle.json is not included in this repository. It must be created and uploaded separately.

In this exercise it is stored in kaggle.json in the Databricks Workspace dbfs : dbfs:/FileStore/kaggle.json - as we don't have access to the Key Vault
In real life project secrets should be stored in eg. Azure Key Vault

### 2) The code is run from kaggle_ex_notebook.py which needs to be imported to the Databricks workspace.
kaggle_ex_notebook imports the code from https://pypi.org/project/kagglebh/1.0.0/

################################### </br>
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


