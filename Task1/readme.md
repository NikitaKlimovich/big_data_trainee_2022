
Converter
=======

This program can convert csv files to parquet and parquet files to csv. Also using this program 
you can view parquet file schema.

Usage
=======
python converter.py [--csv2parquet | --parquet2csv <src-filename> <dst-filename>] 
| [--get-schema <filename>] | [--help] 


Arguments
=======

--csv2parquet: convert csv file <src-filename> to rarquet file <dst-filename>

--parquet2csv: convert from parquet to csv

--get-schema: show schema of parquet file <filename>
	
--help: show help message



Examples of using
=======



python converter.py --csv2parquet userdata.csv userdata.parquet
Convert file "userdata" from csv format to parquet



python converter.py --parquet2csv userdata.parquet userdata.csv
Convert file "userdata" from parquet format to csv



python converter.py --get-schema userdata.parquet
Show schema of file "userdata"
