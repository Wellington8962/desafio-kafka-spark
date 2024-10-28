from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, TimestampType

# Inicializando a SparkSession com suporte ao Kafka
spark = SparkSession.builder \
    .appName("Kafka_Ecommerce_Consumer") \
    .getOrCreate()

# Configurações do Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "vendas_ecommerce"

# Estrutura dos dados JSON recebidos
schema = StructType([
    StructField("id_ordem", StringType()),
    StructField("documento_cliente", StringType()),
    StructField("produtos_comprados", ArrayType(StructType([
        StructField("nome_produto", StringType()),
        StructField("quantidade", IntegerType())
    ]))),
    StructField("valor_total", FloatType()),
    StructField("data_hora_venda", TimestampType())
])

# Lendo o stream de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convertendo o valor das mensagens de Kafka de bytes para string e aplicando o schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Expandindo produtos_comprados para calcular o valor total das vendas por produto
df_exploded = df.withColumn("produto", col("produtos_comprados").getItem(0).getField("nome_produto")) \
    .withColumn("quantidade", col("produtos_comprados").getItem(0).getField("quantidade"))

# Agrupando por produto e somando o valor total das vendas
df_resultado = df_exploded.groupBy("produto") \
    .agg(spark_sum("valor_total").alias("valor_total_vendas"))

# Exibindo o resultado no console
query = df_resultado.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
