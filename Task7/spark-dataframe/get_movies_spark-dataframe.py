import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, regexp_extract, split, explode, avg, row_number

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Get_movies") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def parse(): 
    """
    parse arguments from command line and return them
    """
    parser = argparse.ArgumentParser(description = 'Find the most popular films for specified genres',prog = 'get_movies-spark-dataframe.sh')
    parser.add_argument("-regexp", type = str, help = 'Part of film name')
    parser.add_argument("-year_from", type = int, help = 'Films with year starts from')
    parser.add_argument("-year_to", type = int,  help = 'Films with year ends with')
    parser.add_argument("-genres", type = str, help = 'Films genres')
    parser.add_argument("-input", type = str, help = 'Input files directory')
    parser.add_argument("-output", type = str, help = 'Output files directory')
    parser.add_argument("-N",type = int,help = 'Count of films to show')
    return parser.parse_args()


def to_df(path):
  """
  read csv file and return as dataframe
  """    
  return spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(path)


def get_movies_normalized(movies):
  """
  return movies dataframe with neccesary columns
  """
  movies_normalized = movies.withColumn("year", regexp_extract(col("title"),"\((\d{4})\)", 1)) \
    .withColumn("title", regexp_extract(col("title"),"(.+)\(\d{4}\)", 1)) \
    .withColumn("genres", explode(split("genres",'\|')))
  return movies_normalized


def get_ratings_normalized(ratings):
  """
  return ratings dataframe with neccesary columns
  """
  ratings_normalized=ratings.select('movieId','rating')
  return ratings_normalized


def filter_movies(movies,args):
  """
  return movies dataframe filtered by args
  """
  regexp=args.regexp if args.regexp else ''
  year_from=args.year_from if args.year_from else 0
  year_to=args.year_to if args.year_to else 9999
  genres=args.genres.split('|') if args.genres else None
  if genres:
    return movies.filter(movies.year.between(year_from,year_to)) \
      .filter(movies.title.rlike(regexp)) \
      .filter(movies.genres.isin(genres))
  else:
    return movies.filter(movies.year.between(year_from,year_to)) \
      .filter(movies.title.rlike(regexp))
    

def get_movies_ratings_df(movies,ratings):
  """
  join movies and ratings dataframe and calculate average rating for each film
  """
  df_with_avg_rating = movies.join(ratings,'movieId') \
    .groupBy('movieId') \
      .agg(avg('rating').alias('avg_rating'))      
  return movies.join(df_with_avg_rating,'movieId') \
      .drop("movieId")


def get_first_n(movies_ratings,n):
  """
  get first n movies for each genre 
  """
  if n:
    windowDept = Window.partitionBy("genres") \
      .orderBy(col('avg_rating').desc(),col('year').desc())
    return movies_ratings.withColumn('row',row_number().over(windowDept)) \
      .filter(col("row") <= n) \
      .drop("row")
  else:
    return movies_ratings \
      .sort(col('genres'),col('avg_rating').desc(),col('year').desc())


def save(df,output):
  "save result dataframe as csv file"
  if output==None:
    output='/default'
  res_df = df.select(df.genres, df.title, df.year, df.avg_rating)
  res_df.write.mode('overwrite') \
    .format('csv') \
      .option('header','true') \
      .option('delimiter',';') \
      .option('encoding','utf-8') \
      .option('path',output) \
        .saveAsTable('get_movies')


def main():
  """
  main function
  """
  args = parse()
  input = args.input if args.input else './csv'
  movies_path = input + '/movies.csv'
  ratings_path = input + '/ratings.csv'
  movies=get_movies_normalized(to_df(movies_path))
  ratings=get_ratings_normalized(to_df(ratings_path))
  movies_filtered = filter_movies(movies,args)
  mr = get_movies_ratings_df(movies_filtered,ratings)
  save(get_first_n(mr,args.N),args.output)


if __name__=='__main__':
  main()

