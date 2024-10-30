from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, TimestampType
from py4j.protocol import Py4JError

# Inicializando a SparkSession com suporte ao Kafka
spark = SparkSession.builder \
    .appName("Kafka_Ecommerce_Consumer") \
    .getOrCreate()

# Omitindo os logs de nível "INFO"
spark.sparkContext.setLogLevel("WARN")

# Configurações do Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "vendas_ecommerce"

# Estrutura dos dados JSON recebidos
schema = StructType([
    StructField("id_ordem", StringType()),
    StructField("documento_cliente", StringType()),
    StructField("produtos_comprados", ArrayType(StructType([
        StructField("nome_produto", StringType()),
        StructField("quantidade", IntegerType()),
        StructField("preco_unitario", FloatType())
    ]))),
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

# Expandindo produtos_comprados para acessar nome, quantidade e preco_unitario de cada produto
df_exploded = df.withColumn("produto", col("produtos_comprados").getItem(0).getField("nome_produto")) \
    .withColumn("quantidade", col("produtos_comprados").getItem(0).getField("quantidade")) \
    .withColumn("preco_unitario", col("produtos_comprados").getItem(0).getField("preco_unitario"))

# Calculando o valor total das vendas por produto
df_exploded = df_exploded.withColumn("valor_venda", col("quantidade") * col("preco_unitario"))

# Agrupando por produto e somando o valor total das vendas
df_resultado = df_exploded.groupBy("produto") \
    .agg(
        spark_sum("quantidade").alias("quantidade_total"),
        spark_round(spark_sum("valor_venda"), 2).alias("valor_total_vendas")
    )

try:
    print("\033[92m" + "\n-----------------  INICIANDO CONSUMIDOR -----------------\n" + "\033[0m")
    
    query = df_resultado.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    query.awaitTermination()

except KeyboardInterrupt:
    print("\033[93m" + "\n----------------- CONSUMIDOR INTERROMPIDO PELO USUÁRIO -----------------\n" + "\033[0m")

except Py4JError as e:
    
    if "awaitTermination" in str(e):
        print("\033[93m" + "\n----------------- CONSUMIDOR INTERROMPIDO PELO USUÁRIO -----------------\n" + "\033[0m")
    else:
        print("\033[91m" + f"\n----------------- ERRO -----------------\nOcorreu um erro durante o consumo das mensagens: {e}\n" + "\033[0m")

except Exception as error:
    print("\033[91m" + f"\n----------------- ERRO -----------------\nOcorreu um erro durante o consumo das mensagens: {error}\n" + "\033[0m")

finally:
    print("\033[92m" + "Consumidor finalizado com sucesso.\n" + "\033[0m")
    spark.stop()
