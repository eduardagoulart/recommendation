from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.appName("PySpark").getOrCreate()

imdb = spark.read.csv("datasets/imdb.csv", header=True)
imdb_filter = imdb.selectExpr(
    "Name", "Date AS year_released_imdb", "Genre as genre_imdb"
)

web = spark.read.csv("datasets/web.csv", header=True)
web_filter = web.selectExpr(
    "`Series Title` as Name",
    "`Year Released` AS year_released_web",
    "Genre AS genre_web",
)

all_series = spark.read.csv("datasets/series_data.csv", header=True)
all_series_filter = all_series.selectExpr(
    "`Series_Title` as Name", "`Runtime_of_Series` as runtime", "Genre AS genre_all"
)

def get_year_from_runtime(row):
    print(row)

udf_myFunction = fn.udf(lambda z: get_year_from_runtime(z))
df = all_series_filter.withColumn("message", udf_myFunction("runtime")) #"_3" being the column name of the column you want to consider

# df_web_imdb = web_filter.join(imdb_filter, on="Name", how="outer")
# df = df_web_imdb.join(all_series_filter, on="Name", how="outer")
# df.show(5)
# df = df.withColumn(
#     "year_release",
#     fn.coalesce(
#         fn.col("year_released_web"),
#         fn.coalesce(fn.col("year_released_imdb"), fn.col("runtime")),
#     ),
# )
#
# df = df.withColumn(
#     "year_release",
#     fn.coalesce(
#         fn.col("year_released_web"),
#         fn.coalesce(fn.col("year_released_imdb"), fn.col("runtime")),
#     ),
# )
# df.show(5)
# print(df.select('year_release').distinct().collect())
#
#
