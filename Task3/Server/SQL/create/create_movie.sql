use movies;
drop table if exists lnd_movie;
create table lnd_movie
(m_id 		int 			NOT NULL 	auto_increment,
m_movieid 	int, 
m_title 	varchar(200),  
m_genres 	varchar(100), 
constraint pk_lnd_movie primary key(m_id));