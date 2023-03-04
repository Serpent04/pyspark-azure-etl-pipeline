from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StringType
import spark_funcs


def start_spark_azure(storage_account: str,
                      app_id: str,
                      service_cred: str,
                      dir_id: str):
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('Spark-azure ETL') \
        .config('spark.network.timeout', '3600s') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1') \
        .config('spark.jars.packages', 'com.microsoft.azure:azure-storage:8.6.6') \
        .getOrCreate()

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
                   app_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
                   service_cred)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{dir_id}/oauth2/token")

    return spark


def fill_na_hotels(df):
    new_df = df \
        .withColumn('Latitude', when(df.Latitude.isNull(),
                                     get_lat_udf(df.Address, df.City, df.Country))) \
        .otherwise(df.Latitude) \
        .withColumn('Longitude', when(df.Longitude.isNull(),
                                      get_lon_udf(df.Address, df.City, df.Country))) \
        .otherwise(df.Longitude) \
        .select('Id', 'Name', 'Country', 'City', 'Address', 'Latitude', 'Longitude')
    return new_df


def gen_geohash(df, lat_col: str, lng_col: str, output_cols: list):
    new_df = df \
        .withColumn('Geohash', get_geohash(df[lat_col]), df[lng_col]) \
        .select(output_cols)
    return new_df


def get_result(df_1, df_2):
    result = df_1.join(broadcast(df_2), 'Geohash', 'left')
    result = result \
        .groupBy([c for c in result.columns if c != 'avg_tmpr_f' or c != 'avg_tmpr_c']) \
        .agg(mean('avg_tmpr_f').alias('Temp_F'),
             mean('avg_tmpr_c').alias('Temp_C'))
    return result


# Defining UDFs
get_lat_udf = udf(lambda x, y, z: spark_funcs.get_lat(x, y, z), DoubleType())
get_lon_udf = udf(lambda x, y, z: spark_funcs.get_lon(x, y, z), DoubleType())
get_geohash = udf(lambda x, y: spark_funcs.get_geohash(x, y), StringType())


def main():
    # Initializing credentials from environment variables
    storage_account = 
    app_id = 
    service_cred = 
    dir_id = 
    container_name = 

    # Initializing Spark session
    spark = start_spark_azure(storage_account, app_id, service_cred, dir_id)

    df_hotels = spark.read.csv(f"abfss://{container_name}@{storage_account}"
                               f".dfs.core.windows.net/hotels", header=True, nullValue='NA').cache()
    df_weather = spark.read.parquet(f"abfss://{container_name}@{storage_account}"
                                    f".dfs.core.windows.net/weather", header=True).cache()

    # Handling Hotels table's null values
    hotels = fill_na_hotels(df_hotels)

    # Generating geo-hashes for Hotels table
    hotels_output_cols = ['Geohash', 'Id', 'Name', 'Country', 'City', 'Address']
    hotels_geohash = gen_geohash(hotels, 'Latitude', 'Longitude', hotels_output_cols)

    # Generating geo-hashes for Weather table
    weather_output_cols = ['Geohash', 'lat', 'lng', 'avg_tmpr_f', 'avg_tmpr_c', 'wthr_date', 'year', 'month', 'day']
    weather_geohash = gen_geohash(df_weather, 'lat', 'lng', weather_output_cols)

    # Left Joining Weather with Hotels
    result = get_result(hotels_geohash, weather_geohash)

    # Partitioning the table and writing into Parquet files
    path = r'D:\venvs\spark_hw\parq_data'
    result.write.partitionBy('year', 'month', 'day').parquet(path)


if __name__ == '__main__':
    main()
