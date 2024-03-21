Finder
=======

Find the most popular films for each genre

Usage
=======
python get_movies.py [-genres <genres>] | [-regexp <part_of_title>] | [-year_from <year_from>] | [-year_to <year_to>] | [-N <count>] [--help] 


Arguments
=======

-genres: genres of films to show delimited by |, optional

-regexp: part of film title, optional

-year_from: films from which year to show, optional

-year_to: films to which year to show, optional

-N: count of films with each genre to show, optional
	
--help: show help message



Examples of using
=======


python get_movies.py
Show all films sorted by genre and rating


python get_movies.py.py -regexp Term -N 5
Show top 5 films with "Term" in title


python get_movies.py.py -genres "Action|Comedy" -N 5
Show top 5 films in Action and Comedy genres
