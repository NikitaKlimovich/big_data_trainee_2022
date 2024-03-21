#!/bin/bash
set -e
ARGS=$(getopt -a --options r:f:t:g:n:i:o:h:s --long "regexp:,year_from:,year_to:,genres:,N:,input:,output:,help:,skip:" -- "$@")
eval set -- "$ARGS"
reg="_"
year_f=0
year_t=0
genr="_"
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
      help="$2"
      shift 2;;
    --)
      break;;
     *)
      printf "Unknown option %s\n" "$1" 2>/dev/null
      exit 1;
  esac
done
if [ $mes ] 
then
python ./python/mapper.py -h
else
hdfs dfs -rm -r $input 2>/dev/null
hdfs dfs -rm -r $output 2>/dev/null
hdfs dfs -mkdir $input 
$ hdfs dfs -copyFromLocal ./csv/movies.csv $input
hadoop_streaming_jar=$(sudo find / -name "hadoop-0.23.0-streaming.jar")
hadoop jar $hadoop_streaming_jar \
  -D mapred.map.tasks=4 \
  -D mapred.reduce.tasks=2 \
  -file ./python/mapper.py ./python/reducer.py \
  -mapper "python ./python/mapper.py -regexp "$reg" -year_from $year_f -year_to $year_t -genres $genr" \
  -reducer "python ./python/reducer.py -N $n" \
  -input $input \
  -output $output
fi


