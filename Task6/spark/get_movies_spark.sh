#!/bin/bash

while [ -n "$1" ]
do
case "$1" in

-N) N="-N $2"
shift ;;

--genres|-g) genres="-genres \"$2\""
shift ;;

--year_from|-yf) year_from="-year_from $2"
shift ;;

--year_to|-yt) year_to="-year_to $2"
shift ;;

--regexp|-re) regexp="-regexp $2"
shift ;;

--help|-h) help="-help"
shift ;;

--input|-i) input="-input $2"
shift ;;

--output|-o) output="-output $2"
shift ;;
*) echo "$1 is not an option" 2>/dev/null
exit ;;
esac
shift
done

if [ $help ] 
then
python3 get_movies-spark.py -h
else
hdfs dfs -rm -r $input 2>/dev/null
hdfs dfs -rm -r $output 2>/dev/null
hdfs dfs -mkdir $input 
hdfs dfs -mkdir $output
hdfs dfs -copyFromLocal ./csv/movies.csv $input
hdfs dfs -copyFromLocal ./csv/ratings.csv $input

spark-submit get_movies-spark.py $genres $year_from $year_to $regexp $N $input $output
fi
