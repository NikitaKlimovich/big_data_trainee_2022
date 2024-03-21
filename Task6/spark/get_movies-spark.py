import argparse
import re
from pyspark import SparkContext

def parse(): 
    """
    parse arguments from command line and return them
    """
    parser = argparse.ArgumentParser(description = 'Find the most popular films for specified genres',prog = 'get_movies-local.sh')
    parser.add_argument("-regexp", type = str, help = 'Part of film name')
    parser.add_argument("-year_from", type = int, help = 'Films with year starts from')
    parser.add_argument("-year_to", type = int,  help = 'Films with year ends with')
    parser.add_argument("-genres", type = str, help = 'Films genres')
    parser.add_argument("-input", type = str, help = 'Input files directory')
    parser.add_argument("-output", type = str, help = 'Output files directory')
    parser.add_argument("-N",type = int,help = 'Count of films to show')
    return parser.parse_args()


def get_movies_line_split(line):
       """
       split lines in file with movies into items
       """
       try:
              if '\"' in line:
                     movie_id,title_year, genres = line.replace(',', '').split('\"')
              else:
                     movie_id,title_year, genres = line.split(',')
       except ValueError:
              return []
       try:
              title = re.sub(r'\s\(\d{4}\)', '', title_year)
              year =  re.findall(r'\((\d{4})\)', title_year)[0]
       except IndexError:
              return []
       return movie_id, (title, year, genres)


def get_ratings_line_split(line):
       """
       split lines in file with ratings into items
       """
       try:
              if '\"' in line:
                     _,movie_id, rating, _ = line.replace(',', '').split('\"')
              else:
                     _,movie_id, rating, _ = line.split(',')
              rating_float = float(rating)
       except ValueError:
              return (0,0)
       return (movie_id, rating_float)


def is_correct(line_split,args):
       """
       check if line satisfies a conditions
       """
       try:
              _, (title,year, genres) = line_split
       except ValueError:
              return False
       title = check_title(title, args.regexp)
       year = check_year(int(year), args.year_from, args.year_to)
       genres = check_genre(genres, args.genres)
       return title and year and genres


def check_title(title, regexp):
    if regexp:
        if re.findall(regexp.lower(), title.lower()) != []:
            return title
        else:
            return False
    else:
        return title


def check_year(year, year_from, year_to):
    return check_year_from(year, year_from) and check_year_to(year, year_to)


def check_year_from(year, year_from):
    if year_from:
        return year >= year_from
    else:
        return True


def check_year_to(year, year_to):
    if year_to:
        return year <= year_to
    else:
        return True


def check_genre(genres_all, genres_need):
    if 'no_genres_listed' not in genres_all:
        genres_all=genres_all.replace('\n', '').split('|')
        if genres_need:
            genres_need = genres_need.split('|')
            res = [genre for genre in genres_need if genre in genres_all]
            if res !=[] :
                return True
            else:
                return False
        else:    
            return True
    else:
        return False


def get_avg_rating(res_tuple):
       _, ((title,year,genres),ratings) = res_tuple
       for genre in genres.split('|'):
              yield genre,(title,year,round(sum(ratings)/len(ratings),3))


def get_item(tuple):
       """
       get sorting order
       """
       genre, (_, year, avg_rating) = tuple
       return (genre,avg_rating * -1, year * -1)


def get_group_key(tuple):
       """
       get a key to group by
       """
       genre, (_, year, avg_rating) = tuple
       return genre


def get_first_n(tuple,args):
       """
       get first n values for each group
       """
       count=args.N
       _, item = tuple
       if count:
              return list(item)[:count]
       else:
              return list(item)

def get_format(tuple):
       """
       change to required format
       """
       genre, (title, year, avg_rating) = tuple
       return ";".join([genre,title,year,str(avg_rating)])

def read_file(path):
       """
       read file in hdfs and return it
       """
       sc = SparkContext(master='local[*]')
       return sc.textFile(path)

def get_rdd(args):
       """
       create result rrd and save as text file
       """
       input = args.input
       output = args.output
       try:
              rdd_movies = read_file(input+'/csv/movies.csv')
              rdd_ratings = read_file(input+'/csv/ratings.csv')
       except Exception:
              return 'Error while reading files'

       rdd_movies_mapped_filtered = rdd_movies\
              .map(lambda line: get_movies_line_split(line)) \
              .filter(lambda line_split: is_correct(line_split,args)) #get filtered movies dataset

       rdd_ratings_mapped = rdd_ratings\
              .map (lambda line: get_ratings_line_split(line)) \
              .groupByKey().mapValues(list) #get rating dataset grouped by movie_id

       rdd_movies_ratings = rdd_movies_mapped_filtered\
              .join(rdd_ratings_mapped) \
              .flatMap(lambda res_tuple: get_avg_rating(res_tuple))  #join movie and rating datasets and get average rating

       rdd_movies_ratings_reduced = rdd_movies_ratings\
              .sortBy(lambda tuple:get_item(tuple)) \
              .groupBy(lambda tuple:get_group_key(tuple)) \
              .flatMap(lambda tuple: get_first_n(tuple,args)) #sort by genre and get first n values by rating

       rdd_movies_ratings_result = rdd_movies_ratings_reduced\
              .map(lambda tuple: get_format(tuple)) #get rdd in required format

       rdd_movies_ratings_result.saveAsTextFile(output) #save a result to files

if __name__=='__main__':
       args=parse()
       get_rdd(args)
