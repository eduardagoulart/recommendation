import pyspark.sql.functions as fn

from injestion import run

user_series_list = [
    "Friends",
    "Superstore",
    "Young Sheldon",
    "The Big Bang Theory",
    "Modern Family",
    "The Flight Attendant",
    "YOU",
    "Ted Lasso",
    "Good Girls",
    "Atypical",
    "One Day at a Time",
]


def create_user_profile(df):
    return df.filter(df["Name"].isin(user_series_list))


def get_first_two_genre(df):
    return df.withColumn("genre_1", fn.split(df["genre"], ",").getItem(0)).withColumn(
        "genre_2", fn.split(df["genre"], ",").getItem(1)
    )


df = run()
user = create_user_profile(df)
get_first_two_genre(user).show()
