from pyspark.sql import SparkSession
import pyspark.sql.functions as fn


spark = SparkSession.builder.appName("PySpark").getOrCreate()


def read_all_data():
    imdb = spark.read.csv("datasets/imdb.csv", header=True)
    web = spark.read.csv("datasets/web.csv", header=True)
    all_series = spark.read.csv("datasets/series_data.csv", header=True)
    return imdb, web, all_series


def select_columns_need(imdb, web, all_series):
    """
    Select all necessary columns and rename them
    :param imdb: DF
    :param web: DF
    :param all_series: DF
    :return: three DFs
    """

    imdb_filter = imdb.selectExpr(
        "Name",
        "Date AS year_released_imdb",
        "Genre AS genre_imdb",
        "Rate AS imdb_rating",
    )

    web_filter = web.selectExpr(
        "`Series Title` as Name",
        "`Year Released` AS year_released_web",
        "Genre AS genre_web",
        "`IMDB Rating` AS web_rating",
    )

    all_series_filter = all_series.selectExpr(
        "`Series_Title` AS Name",
        "`Runtime_of_Series` AS runtime",
        "Genre AS genre_all",
        "`IMDB_Rating` AS all_rating",
    )
    return imdb_filter, web_filter, all_series_filter


def get_year_from_runtime(all_series):
    return all_series.withColumn(
        "year", fn.split(all_series["runtime"], "–").getItem(0)
    ).withColumn("year", fn.regexp_replace(fn.col("year"), "[(]", ""))


def join_dfs(imdb, web, all_series):
    df_web_imdb = web.join(imdb, on="Name", how="outer")
    return df_web_imdb.join(all_series, on="Name", how="outer")


def combine_and_filter(df):
    df = df.withColumn(
        "year_release",
        fn.coalesce(
            fn.col("year_released_web"),
            fn.coalesce(fn.col("year_released_imdb"), fn.col("year")),
        ),
    )

    df = df.withColumn(
        "rating",
        fn.coalesce(
            fn.col("web_rating"),
            fn.coalesce(fn.col("imdb_rating"), fn.col("all_rating")),
        ),
    )

    df = df.withColumn(
        "genre",
        fn.coalesce(
            fn.col("genre_web"),
            fn.coalesce(fn.col("genre_all"), fn.col("genre_imdb")),
        ),
    )

    return df.select("Name", "year_release", "genre", "rating")


def run():
    imdb, web, all_series = read_all_data()
    imdb, web, all_series = select_columns_need(imdb, web, all_series)
    all_series = get_year_from_runtime(all_series)
    df = join_dfs(imdb, web, all_series)
    df = combine_and_filter(df)
    return df.dropDuplicates()
