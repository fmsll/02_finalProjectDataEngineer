# Este Spark Application faz parte do projeto final do treinamento Engenheiro de Dados realizado pela Semantix Academy

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from datetime import date, timedelta
from time import sleep

# VARIÁVEIS OBRIGATÓRIAS
# Identificando o dia atual e o dia anterior;
# As duas datas são transformadas em str
today = date.today()
yesterday = date.today() + timedelta(days=-1)
today_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")
# Aqui é escolhido o dia que será usado como base para gerar as visualizações
# Por default o valor é o dia atual, caso queira consulta o dia de ontem pode-se usar trocar para yesterday_str
# Caso queira escolher outra data é necessário especificar uma nova data no formato "yyyy-MM-dd"
usar_dia = yesterday_str
# Condição padrão para filtro das informações do dia anterior
default_filter_condition = 'regiao="Brasil" and data="' + usar_dia + '"'
# Formatação de resultados
formatacao = "{0:.1f}"
formatacao_porcentagem = "{0:.1%}"
# Inicializando Spark Session com suporte ao Hive
spark = (SparkSession
         .builder.appName("ProjetoFinal")
         .config("hive.metastore.uris", "thrift://localhost:10000", conf=SparkConf())
         .enableHiveSupport()
         .getOrCreate()
         )

# Leitura da tabela principal
raw_table = spark.sql("select * from covid19.covid19_particionada_estado")

# Criação de uma View temporária para realizar as queries
raw_table.createOrReplaceTempView("visualizacao1")

# PRIMEIRA VISUALIZAÇÃO
# Query para criação da tabela Hive com as informações: Casos Recuperados, Em Acompanhamento
query_visualizacao1 = 'create table covid19.visualizacao1 SELECT ' \
                      'recuperadosnovos CasosRecuperados, ' \
                      'emacompanhamentonovos EmAcompanhamento' \
                      ' from visualizacao1 where data="' + usar_dia + '" AND regiao="Brasil"'
# Execução da query_visualização1
spark.sql(query_visualizacao1)


# SEGUNDA VISUALIZAÇÃO
# Realiza filtro para buscar o número da população mais atual
filter_populacao = raw_table.filter(default_filter_condition).select("populacaotcu2019").collect()
populacao_total = float(filter_populacao[0].populacaotcu2019)
# Filtro para buscar o número de casos acumulados mais atual
# Extrai o valor do resultado do filtro e converte em float para cálculo da incidência
casos_acumulados = raw_table.filter(default_filter_condition).select("casosacumulado").collect()
value_casos_acumulados = float(casos_acumulados[0].casosacumulado)
# Filtro para buscar o número de casos novos mais atual
# Extrai o valor do resultado
casos_novos = raw_table.filter(default_filter_condition).select("casosnovos").collect()
value_casos_novos = casos_novos[0].casosnovos
# Cálcula a incidência por 100.000: (casos acumulados / populacao total) * 100000
incidencia = formatacao.format((value_casos_acumulados / populacao_total) * 100000)
# Cria o DataFrame da segunda visualização
visualizacao2DF = spark.createDataFrame([(str("{0:.0f}".format(value_casos_acumulados)),
                                          value_casos_novos,
                                          incidencia)],
                                        ("CasosAcumulados", "CasosNovos", "Incidencia")
                                        )
# Salva a segunda visualização em formato parquet com compressão snappy
visualizacao2DF.write.parquet("/user/visualizacao2.parquet", compression="snappy")

# Síntese de casos, óbitos, incidência e mortalidade
# Select realizado na tabela principal para selecionar os dados mais atuais
relatorio = raw_table.select("regiao", "estado", "obitosacumulado","casosacumulado", "populacaotcu2019")\
    .where('data="' + usar_dia + '" and municipio=""')
# Gera a visualização agrupada por Região e Estado
# Cada linha terá o cálculo de Incidência e Mortalidade baseada na população do respectivo estado
relatorio.groupBy(["regiao", "estado"])\
    .agg(max("casosacumulado").alias("Casos Acumulados"),
         max("obitosacumulado").cast("float").alias("Obitos Acumulados"),
         format_number(((max("casosacumulado").cast("float")/max("populacaotcu2019").cast("float"))*100000), 1).alias("Incidência"),
         format_number(((max("obitosacumulado").cast("float")/max("populacaotcu2019").cast("float"))*100000), 1).alias("Mortalidade"),
         )\
    .sort("regiao").show(100)

spark.stop()