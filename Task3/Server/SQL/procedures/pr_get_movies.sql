use movies;
drop procedure if exists pr_get_movies;
delimiter //
create procedure pr_get_movies (IN reg varchar(100), IN year_from int, IN year_to INT, IN genres varchar(100), IN n int)
begin
select 	mr_genre, 
		mr_title, 
        mr_year, 
        mr_avg_rating from
			(select mr_genre, 
					mr_title, 
                    mr_year, 
                    mr_avg_rating, 
                    row_number() over (partition by mr_genre order by mr_avg_rating desc, mr_year desc) as rn 
                    from movie_rating as mr
				where ((mr_genre = regexp_substr(mr_genre,genres) or genres is null) and mr_genre<>'')
                -- and (regexp_like(mr_title, reg) or reg is null)
                and (mr_title regexp(reg) or reg is null)
				and (mr_year>=year_from or year_from is null)
				and (mr_year<=year_to or year_to is null)) as temp
where rn<=n or n is null;
end //