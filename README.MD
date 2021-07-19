PROJETO DE RECRIAÇÃO DOS PAINEIS DISPONIVEIS NO SITE DE INFORMAÇÕES SOBRE A PANDEMIA DE COVID19 NO BRASIL

site: https://covid.saude.gov.br/

O projeto consiste na recriação dos seguintes itens:

1. PRIMEIRA VISUALIZAÇÃO
	- CASOS RECUPERADOS
	- EM ACOMPANHAMENTO
	
Salvar as informações em formato de tabela Hive
	
2. SEGUNDA VISUALIZAÇÃO
	- CASOS CONFIRMADOS ACUMULADOS
	- CASOS NOVOS
	- INCIDÊNCIA SOBRE A CADA 100000 HABITANTES
	
Salvar as informações em arquivo formato parquet e compressão snappy no HDFS

3. TERCEIRA VISUALIZACAO
	- ÓBITOS ACUMULADOS
	- ÓBITOS NAS ÚLTIMAS 24 HORAS (ÓBITOS NOVOS)
	- LETALIDADE / 100mil Hab
	- MORTALIDADE / 100mil Hab
	
Salvar as informações em um tópico KAFKA

4. QUARTA VISUALIZAÇÃO - Síntese de casos, óbitos, incidência e mortalidade
	Recriação de tabela de dados com Casos:
 		- Acumulados
 		- Óbitos acumulados
 		- Incidência
 		- Mortalidade
	Agrupadas por estado e região
	
Para execução desse programa alguns pré-requisitos são necessários:

APLICAÇÃO: HIVE
	- BANCO DE DADOS: 
		-"covid19"
	- TABELAS:
		- "covid19_raw"
		- "covid19_particionada_estado"
		
APLICAÇÃO: KAFKA
	- TOPICO
		- "visualizacao3"
		

IMPORTANTE
----------

As informações sobre a covid19 são disponibilizadas por volta dàs 18 horas todos os dias. 
Por padrão o dia utilizado para realização das consultas é D-1 através da atribuição da varíavel "yesterday_str" a variável "usar_dia"
