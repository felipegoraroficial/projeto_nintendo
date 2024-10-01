import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when, col, regexp_extract, to_date, row_number,udf, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

def silver_magalu():

    path = "/home/fececa/airflow/dags/nintendo/data/bronze/magalu/"

    spark = SparkSession.builder.appName("SilverStep").getOrCreate()


    schema = StructType([
        StructField("titulo", StringType(), True),
        StructField("moeda", StringType(), True),
        StructField("condition_promo", StringType(), True),
        StructField("preco_promo", FloatType(), True),
        StructField("parcelado", FloatType(), True),
        StructField("imagem", StringType(), True),
        StructField("file_date", DateType(), True)
    ])

    df = spark.read.option("multiline", "true").json(path)

    df = df.withColumn("file_name",input_file_name())

    df = df.withColumn("file_date", regexp_extract(col("file_name"), r'\d{4}-\d{2}-\d{2}', 0))

    df = df.withColumn("file_date", to_date(col("file_date"), "yyyy-MM-dd"))

    window_spec = Window.partitionBy("imagem").orderBy(col("file_date").desc())

    df = df.withColumn("row_number", row_number().over(window_spec))

    df = df.filter(col("row_number") == 1).drop("row_number")

    df = df.select("titulo", "moeda", "condition_promo", "preco_promo",
                "parcelado","imagem","file_date")

    df = df.withColumn("parcelado", (col("parcelado").cast("float")))
    df = df.withColumn("preco_promo", (col("preco_promo").cast("float")))

    df = df.withColumn("file_date", to_date(df["file_date"], "yyyy-MM-dd"))

    df = spark.createDataFrame(df.rdd, schema)

    string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

    for col_name in string_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
        df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))

    def extrair_memoria(info):
        import re
        if isinstance(info, str) and info:
            padrao = r'(\d+)\s*(G[gBb])'
            resultado = re.search(padrao, info)
            if resultado:
                return resultado.group(0)
        return '-'

    extrair_memoria_udf = udf(extrair_memoria, StringType())

    df = df.withColumn('memoria', extrair_memoria_udf(col('titulo')))

    df = df.withColumn('oled', when(col('titulo').rlike('(?i)Oled'), 'Sim').otherwise('Nao'))
    df = df.withColumn('lite', when(col('titulo').rlike('(?i)Lite'), 'Sim').otherwise('Nao'))
    df = df.withColumn('joy_con', when(col('titulo').rlike('(?i)Joy-con'), 'Sim').otherwise('Nao'))
    df = df.withColumn('Empresa', lit('Magalu'))

    df.show()

    df.printSchema()

    output_path = '/home/fececa/airflow/dags/nintendo/data/silver/magalu'
    os.makedirs(output_path, exist_ok=True)

    df.write.mode('overwrite').partitionBy('file_date').parquet(output_path)