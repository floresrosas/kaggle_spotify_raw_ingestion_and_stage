### Overview:
This pipeline serves to ingest and stage the Spotify songs dataset available on kaggle:
https://www.kaggle.com/datasets/devdope/900k-spotify
Please review the website for further information on the schema  

Pipeline Overview
![Alt text](chatgpt_pipeline/kaggle_pipeline.drawio.png)

#### How Its Made:
Tools: Python, Requests, Pathlib, Jupyter Notebook, Pyspark, ydata_profiling

##### Raw Data Ingestion
Kaggle API https://www.kaggle.com/datasets/devdope/900k-spotify is used the data from 
the response is returned and stored in the local directory

#### Stage Data 
The local data is then staged and flattened for SQL consupmtion. 
Performing deduplication, 
column formatting, 
data type conversioning 
null handling (assumes values with no release date are not valid 30% of records impacted). The data is written locally in a compressed parquet file and then loaded into a posgres instance 

Some data profiling is performed in the Jupyter notebook to get a general sense of the data but is not necessary in the primary stage script

### SQL exploration
The data is then explored to review the following
- What genre of music is the loudest, most popular and most relaxing 
- ?????
- ??????

#### Optimizations

### General
- This is currently a manual trigger due to requirements stack. Ideally this would be a attached to a pipeline (Airflow, Jenkins)

#### Raw Data Ingestion
- Preference would be to store the data in a data lake for more accessibility and access control
- Some optimizations could be made to the helper functions that create the nested directory paths. Their checks are not through

### Stage Data Ingestion



Known Errors:
