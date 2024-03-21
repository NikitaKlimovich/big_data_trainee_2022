from msilib.schema import Error
import mysql.connector
def connect_to_db(): #set connection to database and return it 
    try:
        connection = mysql.connector.connect(host='localhost',
                                         database='db',
                                         user='root',
                                         password='Nekito2001'
                                         )
        return connection
    except mysql.connector.Error as e:
        print("Error while connecting to MySQL", e)
        return 0
    
def run_sql_script(sql_file): #run some sql script in sql database such as creating table or procedure
    connection=connect_to_db()
    if connection:
        cur=connection.cursor()
        with open(sql_file) as sql_file:
            sql=sql_file.read().replace('//','').replace('delimiter','')
        cur.execute(sql,multi=True)
        connection.close()


def insert_movie_rating(sql_file): #insert data into table "movie_rating"
    connection=connect_to_db()
    if connection:
        cur=connection.cursor()
        with open(sql_file) as sql_file:
            sql=sql_file.read().split(';')
        for each in sql:
            if each.strip()!='':
                cur.execute(each)
        connection.commit()
    connection.close()

def get_movies_tuple(values,row): #get tuple with each item of row in csv file with movies
    if '\"' in row:
        movie_id,title,genres=row.split('\"')
        movie_id=movie_id.replace(",","")
        genres=genres.replace(",","")
    else:
        movie_id,title,genres=row.split(',')
    values.append(tuple([movie_id,title,genres.replace('\n','')]))
    return values

def get_ratings_tuple(values,row): #get tuple with each item of row in csv file with movies
    user_id,movie_id,rating,timestamp=row.split(',')
    values.append(tuple([user_id,movie_id,rating,timestamp.replace('\n','')]))
    return values

def insert_movie(files): #insert data into table "movie"
    values=[]
    connection=connect_to_db()
    csv_file, sql_file = files
    if connection:
        cur=connection.cursor()
        with open(sql_file) as sql_file:
            cur.execute(sql_file.readline())
            sql=sql_file.readline()
        with open(csv_file, 'r', encoding="utf8") as csv_file:
            row=csv_file.readline()
            while row:
                row=csv_file.readline() 
                try:
                    get_movies_tuple(values,row)
                except: 
                    continue
        try:
            cur.executemany(sql,values)
        except: 
            pass
        connection.commit()
        connection.close() 

def insert_rating(files): #insert data into table "rating"
    values=[]
    start=0
    connection=connect_to_db()
    csv_file, sql_file = files
    if connection:
        cur=connection.cursor()
        with open(sql_file) as sql_file:
            cur.execute(sql_file.readline())
            sql=sql_file.readline()
        with open(csv_file, 'r', encoding="utf8") as csv_file:
            row=csv_file.readline()
            while row:
                row=csv_file.readline() 
                try:
                    get_ratings_tuple(values,row)
                except: 
                    continue
        while start<len(values):
            cur.executemany(sql,values[start:start+10000])
            connection.commit()
            start+=10000
        connection.close() 


if __name__=='__main__':
        #path to files
        sql_folder="SQL\\"
        create= 'create\\'
        insert = 'insert\\'
        proc = 'procedures\\'
        csv_folder='CSV\\'
        #creating tables and procedures, inserting data
        try:
            print('Step1')
            run_sql_script(sql_folder+create+'create_database.sql')
            run_sql_script(sql_folder+create+'create_movie.sql')
            run_sql_script(sql_folder+create+'create_movie_rating.sql')
            run_sql_script(sql_folder+create+'create_rating.sql')
            print('Step2')
            insert_movie([csv_folder+'movies_big.csv',sql_folder+insert+'insert_movie.sql'])
            print('Step3')
            insert_rating([csv_folder+'ratings_big.csv',sql_folder+insert+'insert_rating.sql'])
            print('Step4')
            insert_movie_rating(sql_folder+insert+'insert_movie_rating.sql')
            run_sql_script(sql_folder+proc+'pr_get_movies.sql')
            print('Everything loaded successfully')
        except:
            print('Error while loading data ')