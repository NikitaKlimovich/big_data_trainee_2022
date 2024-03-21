use movies;
drop table if exists lnd_rating;
create table lnd_rating
(r_userid	int NOT NULL,
r_movieid 	int NOT NULL, 
r_rating 	decimal(3,1),
r_timestamp varchar(30),   
constraint pk_lnd_rating primary key(r_userid,r_movieid));