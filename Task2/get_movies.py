import argparse
import re
def parse(): 
    """parse arguments from command line and return them"""
    parser=argparse.ArgumentParser()
    parser.add_argument("-regexp",type=str,help='Part of film name')
    parser.add_argument("-year_from",type=int,help='Films with year starts from')
    parser.add_argument("-year_to",type=int, help='Films with year ends with')
    parser.add_argument("-genres",type=str,help='Films genres')
    parser.add_argument("-N",type=int,help='Count of films to show')
    return parser.parse_args()




def unpack(csv_files): 
    """read data from csv files and return an array of dicts"""
    films=[]
    item={}
    ratings={}

    with open(csv_files[1],'r') as f:
        each=f.readline()
        while each:
            each=f.readline()
            try:
                _,movie_id,rating,_=each.split(',')
                if movie_id in ratings.keys():
                    ratings[movie_id][0]+=float(rating)
                    ratings[movie_id][1]+=1
                else: 
                    ratings[movie_id]=[float(rating),1]
            except:
                with open('log_ratings.txt','a',encoding='utf-8') as logs:
                        print(each,file=logs)

    with open(csv_files[0],'r',encoding='utf-8') as f:
        each=f.readline()
        while each:
                each=f.readline()
                try:
                    if '\"' in each:
                        movie_id,title,genres=each.split('\"')
                        movie_id=movie_id.replace(",","")
                        genres=genres.replace(",","")
                    else:
                        movie_id,title,genres=each.split(',')
                    film_name=re.findall(r"(.+)\s\(",title)[0]
                    film_year=re.findall(r"\((\d+)\)",title)[0]
                    film_genres=genres.replace('\n','').split('|')
                    if film_genres==['(no genres listed)']:
                        continue
                    film_rating=round(ratings[movie_id][0]/ratings[movie_id][1],3) #get average film rating
                except:
                    with open('log_films.txt','a',encoding='utf-8') as logs:
                        print(each,file=logs)
                    continue
                for genre in film_genres:
                    item['Name']=film_name
                    item['Year']=int(film_year)
                    item['Genre']=genre
                    item['Rating']=film_rating
                    films.append(item.copy())
    return films

def get_genres(films): 
    """Return all film genres as array"""
    genres=[]
    for film in films:
        if film['Genre'] not in genres:
            genres.append(film['Genre'])
    return genres
        

def find_result(films,args): 
    """find films that satisfy the conditions and return info"""
    res=[]
    #check if argument is None
    if args.genres:
        genres=args.genres.split('|')
    else:
        genres=sorted(get_genres(films))
    if args.regexp:
        regexp=args.regexp
    else:
        regexp=''
    if args.year_from:
        year_from=args.year_from
    else:
        year_from=0
    if args.year_to:
        year_to=args.year_to
    else:
        year_to=0
    if args.N:
        n=args.N
    else:
        n=0
    films=sorted(films,key=lambda film:(film['Genre'],-film['Rating'],-film['Year'],film['Name']))
    res.append('Genre;Name;Year;Rating')

    """find top N films for each genre"""
    for genre in genres:
            k=0
            for film in films:
                name=re.findall(regexp,film['Name'])
                if name!=[] and (film['Year']>=year_from or year_from==0) and (film['Year']<=year_to or year_to==0) and (film['Genre']==genre):
                    res.append(film['Genre']+';'+film['Name']+';'+str(film['Year'])+';'+str(film['Rating']))
                    k+=1
                    if k==n:
                        break
                       
    return res




if __name__ == '__main__':
    folder='files\\'
    films=unpack([folder+'movies.csv',folder+'ratings.csv'])
    args=parse()
    for each in find_result(films,args):
        print(each)

