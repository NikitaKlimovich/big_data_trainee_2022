from pyspark.sql import SparkSession
import argparse

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Get_movies") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def parse(): 
    """
    parse arguments from command line and return them
    """
    parser = argparse.ArgumentParser(description = 'Find the most popular films for specified genres',prog = 'get_movies-spark-sql.sh')
    parser.add_argument("-regexp", type = str, help = 'Part of film name')
    parser.add_argument("-year_from", type = int, help = 'Films with year starts from')
    parser.add_argument("-year_to", type = int,  help = 'Films with year ends with')
    parser.add_argument("-genres", type = str, help = 'Films genres')
    parser.add_argument("-input", type = str, help = 'Input files directory')
    parser.add_argument("-output", type = str, help = 'Output files directory')
    parser.add_argument("-N",type = int,help = 'Count of films to show')
    return parser.parse_args()


def create_table_movies(path):
    """
    create movies table from csv file
    """
    command = f"create table movies \
    (movieId int, title string, genres string) \
        using csv options(header = true, \
        path = '{path}')" 
    spark.sql(command)


def create_table_ratings(path):
    """
    create ratings table from csv file
    """
    command = f"create table ratings \
    (userId int, movieId int, rating float, timestamp string) \
        using csv options(header = true, \
        path = '{path}')" 
    spark.sql(command)


def get_movies_normalized():
    """
    create movies view with neccesary columns
    """
    command = r"""create view movies_normalized 
    as select movieId, 
        regexp_extract(title,'(.+)\\(\\d{4}\\)',1) as title, 
        regexp_extract(title,'\\((\\d{4})\\)',1) as year,
        explode(split(genres,'\\|')) as genre
        from movies""" 
    spark.sql(command)


def get_ratings_normalized():
    """
    create ratings view with neccesary columns
    """
    command = """create view ratings_normalized 
    as select movieId, rating
        from ratings""" 
    spark.sql(command)


def filter_movies(args):
    """
    create movies view filtered by args
    """
    regexp=args.regexp if args.regexp else ''
    year_from=args.year_from if args.year_from else 0
    year_to=args.year_to if args.year_to else 9999
    genres=args.genres if args.genres else ''
    command = f"""create view movies_filtered as
            select *
            from movies_normalized
            where (rlike(title,'{regexp}'))
            and (year >= {year_from})
            and (year <= {year_to})
            and (rlike('{genres}',genre))"""
    spark.sql(command)


def get_movies_ratings():
    """
    create view as join movies and ratings dataframe and calculate average rating for each film
    """
    command = f"""create view movies_ratings as 
          select mf.movieId, title, year, genre, rating
          from movies_filtered mf
          inner join ratings_normalized rn
          on mf.movieId = rn.movieId
          """
    spark.sql(command)
    command = f"""create view movies_avg_ratings 
          as select distinct genre, title, year, avg_rating
          from movies_ratings mr1
          join (
            select movieId, avg(rating) as avg_rating
            from movies_ratings
            group by movieId) as mr2
          on mr1.movieId = mr2.movieId
          """
    spark.sql(command)
 
  
def get_first_n(args):
    """
    get first n movies for each genre 
    """
    n=args.N
    output = args.output if args.output else 'result'
    if n:
      command = f"""create table {output} using csv
            as select genre, title, year, avg_rating
            from 
            (select genre, title, year, avg_rating, 
            row_number() over(partition by genre order by avg_rating desc, year desc) as rn
            from movies_avg_ratings)
            where rn<={n}
            """
    else:
        command = f"""create table {output} using csv
            as select genre, title, year, avg_rating
            from movies_avg_ratings
            order by genre, avg_rating desc, year desc
            """
    spark.sql(command) 


def main():
    """main function"""
    args = parse()
    movies_path=args.input+'/movies.csv'
    ratings_path=args.input+'/ratings.csv'
    create_table_movies(movies_path)
    create_table_ratings(ratings_path)
    get_movies_normalized()
    get_ratings_normalized()
    filter_movies(args)
    get_movies_ratings()
    get_first_n(args)

if __name__=='__main__':
    main()

