# Este Spark Application faz parte do projeto final do treinamento Engenheiro de Dados realizado pela Semantix Academy

from pyspark.sql import *
from pyspark.conf import SparkConf
from time import sleep
from datetime import date, timedelta

# Variáveis Obrigatórias

# Inicializando Spark Session com suporte ao Hive
spark = (SparkSession
         .builder.appName("ProjetoFinal")
         .config("hive.metastore.uris", "thrift://localhost:10000", conf=SparkConf())
         .enableHiveSupport()
         .getOrCreate()
         )
# Identificando o dia atual e o dia anterior;
# As duas datas são transformadas em str
today = date.today()
yesterday = date.today() + timedelta(days=-1)
today_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")

# Leitura da tabela principal
raw_table = spark.sql("select * from covid19.covid19_particionada_estado")

# Criação de uma View temporária para realizar as queries
raw_table.createOrReplaceTempView("visualizacao1")

# Primeira Visualização
# Query para criação da tabela Hive com as informações: Casos Recuperados, Em Acompanhamento
query_visualizacao1 = 'create table covid19.visualizacao1 SELECT ' \
                      'recuperadosnovos CasosRecuperados, ' \
                      'emacompanhamentonovos EmAcompanhamento' \
                      ' from visualizacao1 where data="' + yesterday_str + '" AND regiao="Brasil"'
# Execução da query_visualização1
spark.sql(query_visualizacao1)
sleep(5)



spark.stop()