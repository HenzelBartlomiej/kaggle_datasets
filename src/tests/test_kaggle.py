import os
import pyspark
import unittest

from pyspark.sql.functions import col, max


# kaggle api example unit tests
# requires spark to be installed on a local machine/test environment
# requires kaggle credentials for test 2 - etsting kaggle connection


# test data
test_data_HEADER= ["dt", "AverageTemperature", "AverageTemperatureUncertainty", "State","Country" ]
test_data=[
    ("1855-05-01",25.544,1.171,"Acre","Brazil"),
    ("1855-06-01",24.228,1.103,"Acre","Brazil"),
    ("1855-07-01",24.371,1.044,"Acre","Brazil"),
    ("1855-08-01",25.427,1.073,"Acre","Brazil"),
    ("1855-09-01",25.675,1.014,"Acre","Brazil"),
    ("1881-09-01",26.785,1.085,"Acre","Brazil"),
    ("1881-10-01",26.287,1.005,"Acre","Brazil"),
    ("1881-11-01",25.358,1.063,"Acre","Brazil"),
    ("1881-12-01",26.171,1.198,"Acre","Brazil"),
    ("1882-01-01",26.263,1.057,"Acre","Brazil"),
    ("2012-01-01",4.91,0.324,"Zhejiang","China"),
    ("2012-02-01",5.523,0.332,"Zhejiang","China"),
    ("2012-03-01",10.819,0.305,"Zhejiang","China"),
    ("2012-04-01",17.977,0.386,"Zhejiang","China"),
    ("2012-05-01",21.28,0.217,"Zhejiang","China"),
    ("2012-06-01",24.02,0.509,"Zhejiang","China"),
    ("2012-07-01",28.732,0.4,"Zhejiang","China"),
    ("2012-08-01",27.704,0.205,"Zhejiang","China"),
    ("2012-09-01",23.024,0.289,"Zhejiang","China"),
    ("2012-10-01",18.864,0.412,"Zhejiang","China")
]

spark = pyspark.sql.SparkSession.builder.appName("test_app1").getOrCreate()
test_df = spark.createDataFrame(test_data, test_data_HEADER)


class TestConnection(unittest.TestCase):

    def test_environment_variables(self):
        os.environ['KAGGLE_USERNAME'] = 'yoda'
        os.environ['KAGGLE_KEY'] = 'master'
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()

        # We haven't authenticated yet
        self.assertTrue("key" not in api.config_values)
        self.assertTrue("username" not in api.config_values)
        api.authenticate()

        # Should be set from the environment
        self.assertEqual(api.config_values['username'], 'yoda')
        self.assertEqual(api.config_values['key'], 'master')

    def test_connection(self):
        os.environ['KAGGLE_USERNAME'] = 'your_username'
        os.environ['KAGGLE_KEY'] = 'your_key'
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        print(api.dataset_list_files("berkeleyearth/climate-change-earth-surface-temperature-data").files)
        self.assertTrue(api.dataset_list_files("berkeleyearth/climate-change-earth-surface-temperature-data").files!=[])

    def test_df(self):
        df_agg = test_df.groupBy("Country", "State").agg(max(col("AverageTemperature")).alias("MaxAverageTemperature"))
        df_agg.show()
        self.assertTrue(df_agg.count()==2)
        
if __name__ == '__main__':
    unittest.main()
