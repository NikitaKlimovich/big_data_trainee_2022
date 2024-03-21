import mysql.connector
import argparse
def parse(): #parse arguments from command line and return them
    parser=argparse.ArgumentParser()
    parser.add_argument("-regexp",type=str,help='Part of film name')
    parser.add_argument("-year_from",type=int,help='Films with year starts from')
    parser.add_argument("-year_to",type=int, help='Films with year ends with')
    parser.add_argument("-genres",type=str,help='Films genres')
    parser.add_argument("-N",type=int,help='Count of films to show')
    return parser.parse_args()
def connect_to_db(): #set connection to database and return it
    try:
        connection = mysql.connector.connect(host='localhost',
                                         database='movies',
                                         user='root',
                                         password='Nekito2001')
        if connection.is_connected():
            return connection

    except mysql.connector.Error as e:
        print("Error while connecting to MySQL", e)

#@profile
def get_callproc_res(proc_name): #get data returned by calling prodecure
    args=tuple(vars(parse()).values())
    connection=connect_to_db()
    cur=connection.cursor()
    cur.callproc(proc_name,args)
    return transform(cur.stored_results())
def transform(call_proc_res): #transforming data into required format
    data=['genre;title;year;rating']
    res_str=''
    for result in call_proc_res:
        for row in result.fetchall():
            for item in row:
               res_str+=str(item).strip()+';'
            data.append(res_str)
            res_str=''
    return data 




if __name__ == '__main__':
        for row in get_callproc_res('pr_get_movies'):
            print(row)


