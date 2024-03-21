use movies;
INSERT INTO movie_rating (
mr_title, 
mr_year, 
mr_genre, 
mr_avg_rating
)
WITH 
RECURSIVE numbers 
AS (
	SELECT 1 AS n 
	UNION 
	DISTINCT SELECT 1+n 
	FROM numbers 
	INNER JOIN lnd_movie AS m
	ON CHAR_LENGTH(m.m_genres)-CHAR_LENGTH(REPLACE(m.m_genres, '|', ''))>=numbers.n-1)
SELECT 	m_title, 
		m_year, 
		SUBSTRING_INDEX(SUBSTRING_INDEX(temp.m_genres, '|', numbers.n), '|', -1) as m_genre, 
		m_avg_rating
FROM numbers 
INNER JOIN 
	(SELECT regexp_replace(m_title,'[(][0-9]{4}[)]','') AS m_title, 
			IF (m_title REGEXP('[(][0-9]{4}[)]'), regexp_substr(regexp_substr(m_title,'[(][0-9]{4}[)]'),'[0-9]{4}'),'') AS m_year, 
			IF (m_genres REGEXP ('no genres listed'),'',m_genres) AS m_genres, 
			AVG(r_rating) AS m_avg_rating 
    FROM lnd_movie AS m
	INNER JOIN lnd_rating AS r 
    ON m.m_movieid=r.r_movieid
	GROUP BY m.m_movieid) 
    AS temp
	ON CHAR_LENGTH(temp.m_genres)-CHAR_LENGTH(REPLACE(temp.m_genres, '|', ''))>=numbers.n-1;