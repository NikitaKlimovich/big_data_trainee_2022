#!/bin/bash
set -e
ARGS=$(getopt -a --options r:f:t:g:n:i:o:h:s --long "regexp:,year_from:,year_to:,genres:,N:,input:output:help:skip:" -- "$@")
eval set -- "$ARGS"
reg="_"
year_f=0
year_t=0
genr="_"
help=""
n=0
while true; do
  case "$1" in
    -r |--regexp)
      reg="$2"
      shift 2;;
    -f |--year_from)
      year_f="$2"
      shift 2;;
    -t |--year_to)
      year_t="$2"
      shift 2;;
    -g |--genres)
      genr="$2"
      shift 2;;
    -n |--N)
      n="$2"
      shift 2;;
    -i |--input)
      input="$2"
      shift 2;;
    -o |--output)
      output="$2"
      shift 2;;
    -h |--help)
      mes="help"
      shift 2;;
    --)
      break;;
     *)
      printf "Unknown option %s\n" "$1"
      exit 1;
  esac
done
printf "genre;title;year\n"
if [ $mes ] 
then
python ./python/mapper.py -h
else
cat ./csv/movies.csv | python ./python/mapper.py -regexp "$reg" -year_from $year_f -year_to $year_t -genres $genr | sort | python ./python/reducer.py -N $n 
fi

