import argparse
import re
import sys


def parse(): 
    """
    parse arguments from command line and return them
    """
    parser = argparse.ArgumentParser(description = 'Find newest films for specified genres',prog = 'get_movies-local.sh')
    parser.add_argument("-regexp", type = str, help = 'Part of film name')
    parser.add_argument("-year_from", type = int, help = 'Films with year starts from')
    parser.add_argument("-year_to", type = int,  help = 'Films with year ends with')
    parser.add_argument("-genres", type = str, help = 'Films genres')
    return parser.parse_args()


def map(args, line):
    """
    Get a line from csv file and return genre and tuple of year and title
    """
    try:
        if '\"' in line:
            _, title_year, genres_all = line.replace(',', '').split('\"')
        else:
            _, title_year, genres_all = line.split(',')
    except ValueError:
        return
    title = check_title(title_year, args.regexp)
    year = check_year(title_year, args.year_from, args.year_to)
    genres = check_genre(genres_all, args.genres)
    if (title and year and genres):
        for genre in genres:
            yield genre, (year, title)


def check_title(title_year, regexp):
    """
    Check if movie info satisfies command line arguments and return the argument or False
    """
    title = re.sub(r'\s\(\d{4}\)', '', title_year)
    if regexp != '_':
        if re.findall(regexp.lower(), title.lower()) != []:
            return title
        else:
            return False
    else:
        return title


def check_year(title_year, year_from, year_to):
    try:
        year = int(re.findall(r'\((\d{4})\)', title_year)[0])
    except IndexError:
        return False
    if check_year_from(year, year_from) and check_year_to(year, year_to):
        return year
    else:
        return False


def check_year_from(year, year_from):
    if year_from != 0:
        return year >= year_from
    else:
        return True


def check_year_to(year, year_to):
    if year_to != 0:
        return year <= year_to
    else:
        return True


def check_genre(genres_all, genres_need):
    if 'no_genres_listed' not in genres_all:
        genres_all=genres_all.replace('\n', '').split('|')
        if genres_need != '_':
            genres_need = genres_need.split('|')
            res = [genre for genre in genres_need if genre in genres_all]
            if res !=[] :
                return res
            else:
                return False
        else:    
            return genres_all
    else:
        return False


if __name__=='__main__':
    args=parse()
    """
    Print result in required format
    """
    for line_number, line in enumerate(sys.stdin): 
        for key, value in map(args, line): 
            print("{}\t{}".format(key, value)) 
