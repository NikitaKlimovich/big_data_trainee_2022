import argparse
import sys
from ast import literal_eval


def parse(): 
    """parse arguments from command line and return them"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-N",type = int,help = 'Count of films to show')
    return parser.parse_args()


def shuffle(num_reducers=1):
    """Groups result given from mapper by year"""
    values = []
    shuffled_items = []
    pred_key = ''
    for line in sys.stdin:
        key, value = line.split('\t')
        if key!=pred_key and pred_key != '':
            shuffled_items.append("{}\t{}".format(pred_key,values))
            values = []
        pred_key = key
        values.append(value)
    shuffled_items.append("{}\t{}".format(pred_key,values))
    result = []
    num_items_per_reducer = len(shuffled_items) // num_reducers
    if len(shuffled_items) / num_reducers != num_items_per_reducer:
        num_items_per_reducer += 1
    for i in range(num_reducers):
        result.append(shuffled_items[num_items_per_reducer*i:num_items_per_reducer*(i+1)])

    return result


def reducer(key,value):
    """Get genre as key and tuple of year&title as value and return first n pairs sorted by year in descending order"""
    args=parse()
    count = args.N
    values = literal_eval(value)
    values.reverse()
    if count == 0:
        return key,values
    return key,values[:count]      


if __name__ == '__main__':
    """Print result in required format"""
    for item in shuffle():
        for element in item:
            key, value = element.split('\t')
            res_key, res_value = reducer(key,value)
            for item in res_value:
                year,title=literal_eval(item)
                print(",".join([key,title,str(year)]))
    