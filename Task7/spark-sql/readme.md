Finder
=======

Find the most popular films for each genre

Usage
=======
First install necessary libraries:  
$ pip install -r requirements

Then run program:  

$ get_movies_spark-sql.sh [--genres <genres>] | [--regexp <part_of_title>] | [--year_from <year_from>] | [--year_to <year_to>] | [--N <count>] [--help] [--input <input path>]  [--output <output table name>]    

Arguments
=======

-genres: genres of films to show delimited by |, optional

-regexp: part of film title, optional

-year_from: films from which year to show, optional

-year_to: films to which year to show, optional

-N: count of films with each genre to show, optional

-input: input path in hdfs storage

-output: table name to output
	
--help: show help message



Examples of using
=======
get_movies_spark-sql.sh --input /usr/nikita --output nikita   
Write all films sorted by genre and rating  


get_movies_spark-sql.sh -regexp Term -N 5  --input /usr/nikita --output nikita 
Write top 5 films with "Term" in title  


get_movies_spark-sql.sh -genres "Action|Comedy" -N 5  --input /usr/nikita --output nikita 
Write top 5 films in Action and Comedy genres  
