Finder
=======

Find the newest films for each genre

Usage
=======
You can run this program using your local machine or install hadoop and run using it.  

$ get_movies-local.sh [-genres <genres>] | [-regexp <part_of_title>] | [-year_from <year_from>] | [-year_to <year_to>] | [-N <count>] [--help] #run locally  

$ get_movies-hadoop.sh [-genres <genres>] | [-regexp <part_of_title>] | [-year_from <year_from>] | [-year_to <year_to>] | [-N <count>] [--help] [-input <input path>]  [-output <output path>]#run on hadoop and specify output path

Arguments
=======

-genres: genres of films to show delimited by |, optional

-regexp: part of film title, optional

-year_from: films from which year to show, optional

-year_to: films to which year to show, optional

-N: count of films with each genre to show, optional

-input: input path in hdfs storage

-output: output path in hdfs storage
	
--help: show help message



Examples of using
=======
get_movies-hadoop.sh  --input /usr/nikita --output /usr/nikita/result
Show all films sorted by genre and rating  


get_movies-hadoop.sh -regexp Term -N 5  --input /usr/nikita --output /usr/nikita/result
Show top 5 films with "Term" in title  


get_movies-hadoop.sh -genres "Action|Comedy" -N 5  --input /usr/nikita --output /usr/nikita/result
Show top 5 films in Action and Comedy genres  
