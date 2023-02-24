# spark-azure
ETL pipeline from Azure Blob Storage to local storage.
The task was to pull data from the Azure Container. Clean it, handle null's, enrich data by joining 
two DataFrames and then save the final result locally, avoiding data duplication and partition the data prior to the saving process.

Connection to the container was implemented via OAuth2.0.

Additional libraries hadoop-azure and azure-storage are needed for a succesfull connection.

In this project Spark 3.3.1 and Hadoop 3.3.4 are used. 
Repo with the used winutils: https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin
