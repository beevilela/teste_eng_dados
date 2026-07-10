
import boto3

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType,
)
from pyspark.sql.window import Window



# 1. CONFIGURAÇÕES


SOURCE_PATH = "datasets/clientes_sinteticos.csv"

BRONZE_PATH = "s3://bucket-bronze/tabela_cliente_landing"
SILVER_PATH = "s3://bucket-silver/tb_cliente"

GLUE_DATABASE = "clientes_database"
BRONZE_TABLE = "tabela_cliente_landing"
SILVER_TABLE = "tb_cliente"

PROCESSING_DATE = datetime.now().strftime("%Y%m%d")

PHONE_PATTERN = r"^\(\d{2}\)\d{5}-\d{4}$"



# 2. CRIAÇÃO DA SESSÃO SPARK


spark = (
    SparkSession.builder
    .appName("etl_clientes_bronze_silver")
    .getOrCreate()
)



# 3. DEFINIÇÃO DO SCHEMA


schema = StructType(
    [
        StructField("cod_cliente", IntegerType(), False),
        StructField("nm_cliente", StringType(), False),
        StructField("nm_pais_cliente", StringType(), False),
        StructField("nm_cidade_cliente", StringType(), False),
        StructField("nm_rua_cliente", StringType(), False),
        StructField("num_casa_cliente", IntegerType(), False),
        StructField("telefone_cliente", StringType(), False),
        StructField("dt_nascimento_cliente", DateType(), False),
        StructField("dt_atualizacao", DateType(), False),
        StructField("tp_pessoa", StringType(), False),
        StructField("vl_renda", DecimalType(12, 2), False),
    ]
)



# 4. LEITURA DO CSV


df_origem = (
    spark.read
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(schema)
    .csv(SOURCE_PATH)
)



# 5. TRANSFORMAÇÕES DA CAMADA BRONZE


df_bronze = (
    df_origem
    .withColumn(
        "nm_cliente",
        F.upper(F.trim(F.col("nm_cliente"))),
    )
    .withColumnRenamed(
        "telefone_cliente",
        "num_telefone_cliente",
    )
)



# 6. ESCRITA DA BRONZE NO S3


bronze_partition_path = (
    f"{BRONZE_PATH}/anomesdia={PROCESSING_DATE}"
)

(
    df_bronze.write
    .mode("overwrite")
    .format("parquet")
    .save(bronze_partition_path)
)



# 7. REGISTRO DA PARTIÇÃO BRONZE NO GLUE


glue_client = boto3.client("glue")

bronze_table_response = glue_client.get_table(
    DatabaseName=GLUE_DATABASE,
    Name=BRONZE_TABLE,
)

bronze_storage = bronze_table_response["Table"]["StorageDescriptor"]

bronze_partition = {
    "Values": [PROCESSING_DATE],
    "StorageDescriptor": {
        "Columns": bronze_storage["Columns"],
        "Location": bronze_partition_path,
        "InputFormat": bronze_storage.get("InputFormat"),
        "OutputFormat": bronze_storage.get("OutputFormat"),
        "SerdeInfo": bronze_storage.get("SerdeInfo"),
        "Compressed": bronze_storage.get("Compressed", False),
        "Parameters": bronze_storage.get("Parameters", {}),
    },
}

bronze_response = glue_client.batch_create_partition(
    DatabaseName=GLUE_DATABASE,
    TableName=BRONZE_TABLE,
    PartitionInputList=[bronze_partition],
)

for error in bronze_response.get("Errors", []):
    error_code = error.get("ErrorDetail", {}).get("ErrorCode")

    if error_code != "AlreadyExistsException":
        raise RuntimeError(
            f"Erro ao registrar a partição Bronze: {error}"
        )



# 8. DEDUPLICAÇÃO PARA A CAMADA SILVER


janela_cliente = (
    Window
    .partitionBy("cod_cliente")
    .orderBy(F.col("dt_atualizacao").desc())
)

df_silver = (
    df_bronze
    .withColumn(
        "ordem_atualizacao",
        F.row_number().over(janela_cliente),
    )
    .filter(F.col("ordem_atualizacao") == 1)
    .drop("ordem_atualizacao")
)



# 9. VALIDAÇÃO DO TELEFONE


df_silver = (
    df_silver
    .withColumn(
        "num_telefone_cliente",
        F.when(
            F.col("num_telefone_cliente").rlike(PHONE_PATTERN),
            F.col("num_telefone_cliente"),
        ).otherwise(
            F.lit(None).cast(StringType())
        ),
    )
)



# 10. ESCRITA DA SILVER NO S3


silver_partition_path = (
    f"{SILVER_PATH}/anomesdia={PROCESSING_DATE}"
)

(
    df_silver.write
    .mode("overwrite")
    .format("parquet")
    .save(silver_partition_path)
)



# 11. REGISTRO DA PARTIÇÃO SILVER NO GLUE


silver_table_response = glue_client.get_table(
    DatabaseName=GLUE_DATABASE,
    Name=SILVER_TABLE,
)

silver_storage = silver_table_response["Table"]["StorageDescriptor"]

silver_partition = {
    "Values": [PROCESSING_DATE],
    "StorageDescriptor": {
        "Columns": silver_storage["Columns"],
        "Location": silver_partition_path,
        "InputFormat": silver_storage.get("InputFormat"),
        "OutputFormat": silver_storage.get("OutputFormat"),
        "SerdeInfo": silver_storage.get("SerdeInfo"),
        "Compressed": silver_storage.get("Compressed", False),
        "Parameters": silver_storage.get("Parameters", {}),
    },
}

silver_response = glue_client.batch_create_partition(
    DatabaseName=GLUE_DATABASE,
    TableName=SILVER_TABLE,
    PartitionInputList=[silver_partition],
)

for error in silver_response.get("Errors", []):
    error_code = error.get("ErrorDetail", {}).get("ErrorCode")

    if error_code != "AlreadyExistsException":
        raise RuntimeError(
            f"Erro ao registrar a partição Silver: {error}"
        )



# 12. FINALIZAÇÃO


print("ETL concluído com sucesso.")
print(f"Partição processada: anomesdia={PROCESSING_DATE}")

spark.stop()
