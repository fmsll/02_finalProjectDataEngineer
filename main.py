# Este Spark Application faz parte do projeto final do treinamento Engenheiro de Dados realizado pela Semantix Academy

from pyspark.sql import *
from pyspark.conf import SparkConf
from time import sleep
from datetime import date

# Variáveis Obrigatórias

# Inicializando Spark Session com suporte ao Hive
spark = (SparkSession
         .builder.appName("ProjetoFinal")
         .config("hive.metastore.uris", "thrift://localhost:10000", conf=SparkConf())
         .enableHiveSupport()
         .getOrCreate()
         )
# Identificando o dia atual e fazendo um cast para str
today = str(date.today())
# Leitura da tabela principal
raw_table = spark.sql("select * from covid19.covid19_particionada_estado")


sleep(5)

spark.stop()