use movies;
DROP TABLE IF EXISTS movie_rating;
CREATE TABLE movie_rating (
    mr_id INT NOT NULL AUTO_INCREMENT,
    mr_title VARCHAR(200) NOT NULL,
    mr_year VARCHAR(5) NOT NULL,
    mr_genre VARCHAR(30) NOT NULL,
    mr_avg_rating FLOAT(7 , 4 ) NOT NULL,
    CONSTRAINT pk_movie_rating PRIMARY KEY (mr_id)
);