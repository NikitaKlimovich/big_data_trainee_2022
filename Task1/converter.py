import argparse
import pandas as pd
import pyarrow.parquet as pq



def parse(): 
    """parse arguments from command line and return them"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv2parquet', nargs=2, metavar=('src-filename','dst-filename'), help='Convert from csv to parquet')
    parser.add_argument('--parquet2csv',  nargs=2, metavar=('src-filename','dst-filename'), help='Convert from parquet to csv')
    parser.add_argument('--get-schema',  metavar='filename', help='Get parquet file schema')
    return parser.parse_args()



def csv2parquet(csv_file,parquet_file): 
    """convert file from csv format to parquet"""
    try:
        file = pd.read_csv(csv_file,low_memory=False)
    except:
        print('Reading file error')
        return
    file.to_parquet(parquet_file)




def parquet2csv(parquet_file,csv_file): 
    """convert file from parquet format to csv"""
    try:
        file = pd.read_parquet(parquet_file)
    except:
        print('Reading file error')
        return
    file.to_csv(csv_file)




def get_schema(parquet_file): 
    """get schema of parquet file"""
    try:
        return pq.read_schema(parquet_file).remove_metadata()
    except:
        print('Getting schema error')




def main():
    args=parse()
    if args.csv2parquet:
        filename_to, filename_from = args.csv2parquet
        csv2parquet(filename_to,filename_from)
    if args.parquet2csv:
        filename_to, filename_from = args.parquet2csv
        parquet2csv(filename_to,filename_from)
    if args.get_schema:
        print(get_schema(args.get_schema))



if __name__ == '__main__':
    main()
