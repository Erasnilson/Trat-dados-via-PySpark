# Trat-dados-via-PySpark
Extração de dados de demanda de voo ANAC via Pyspark


Principais comandos utilizados:

# Renomear as variáveis
from pyspark.sql.functions import *
df = df.withColumnRenamed("Referência","Data").withColumnRenamed("count(Referência)","n")
df.printSchema()
df = df.toPandas()
df

# deixar a variavel referencia no mesmo padrao das demais
from pyspark.sql.functions import *
data_voov2 = data_voo.withColumn('Referência', to_date(data_voo['Referência'], 'yyyy-MM-dd'))
df_train = df_train.withColumn('Data_ChegadaR', to_timestamp(col('Chegada Real'),'dd/MM/yyyy HH:mm'))


# deletar coluna que possui null
df_train = df_train.dropna(subset=('Partida Real','Chegada Real','Referência'))

# subdividir uma variavel por carachter
df_train=df_train.withColumn('year', substring('Referência', 1,4))\
.withColumn('month', substring('Referência', 6,2))\
.withColumn('day', substring('Referência', 9,2))                     
df_train.show(5)
# 4 digitos (incluíndo 1) = ano  ex: 2024-06-01
# 2 digitos (incluíndo 6) = mês
# 2 digitos (incluíndo 9) = mês


# fitros
df.filter(df.age > 3).show()
df.filter("age > 2").show()
df.where("age = 2").show()

df_train.groupBy('year','month').agg({"Referência": "count"}).sort('year','month').toPandas()
df_train.groupBy('Situação Chegada',"Empresa Aérea","DiffInHoursR").count().show()
df_train.groupBy('Situação Chegada',"Empresa Aérea","Data_Ref").agg({"horas_voo": "avg"}).sort("Empresa Aérea").show()
'Sigla ICAO Aeroporto Destino'Referência .sort('year','month')
.sort("Situação Voo", "Número de Assentos")
.agg({"salary": "avg", "age": "max"}).show()


#visualizando as estatísticas básicas das colunas
pd_df.describe()


filtro pandas
iltro_combinado = dados[(dados['idade'] > 30) & (dados['cidade'] == 'São Paulo')]
