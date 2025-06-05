import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

#configs
spark = (
    SparkSession.builder
        .master("local")
        .appName("Practice Kaggle Data Read")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
)
json_location = "/Users/jasminewilliams/git/chatgpt_exam/chatgpt_pipeline/data/unzip/900k Definitive Spotify Dataset.json"


def read(file_location):
    df = spark.read.json(file_location)
    return df

def format_columns(df):
    format_v1_cols = [c.lower().replace(" ","_") for c in df.columns]
    formatted_cols = [c.replace('(','').replace(')','').replace('/',"_") for c in format_v1_cols]
    df = df.toDF(*formatted_cols)
    return df

def data_type_conversion(df):
    misconfigured_types = {
        "release_date": "timestamp",
        "popularity": "int",
        "positiveness": "int",
        "energy": "int",
        "danceability": "int",
        "speechiness": "int",
        "liveness": "int",
        "acousticness": "int",
        "instrumentalness": "int"
    }

    for column_name, new_type in misconfigured_types.items():
        df = df.withColumn(column_name, f.col(column_name).cast(new_type))
    
    return df

def remove_nulls(df):   
    return df.filter('release_date is not null')

def dedup(df):
    return df.drop_duplicates()


def write_to_parquet(df, output_location):
    partition_cols = ["artists", "song", "genre", "release_date"]
    df = df.repartition(*partition_cols)
    df.write.parquet(output_location)

def write_to_posgres(df, output_location):
    partition_cols = ["artists", "song", "genre", "release_date"]
    df = df.repartition(*partition_cols)
    df.write.parquet(output_location)

def write_to_sql(df):
    df = spark.read.parquet("path/to/your_file.parquet")

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/your_db") \
    .option("dbtable", "your_table") \
    .option("user", "your_username") \
    .option("password", "your_password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \  # or 'append'
    .save()

def data_validation(df):
    assert(df.filter('release_date is null').isEmpty())

if __name__ == "__main__":
    df = read(json_location)
    df = format_columns(df)
    df = data_type_conversion(df)
    df = remove_nulls(df)
    df = dedup(df)
    output_location = "chatgpt_pipeline/data/parquet/"
    write_to_parquet(df, output_location)
