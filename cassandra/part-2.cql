create table videos_by_title_year(
	title text, 
	added_year int, 
	added_date timestamp, 
	description text, 
	user_id uuid, 
	video_id timeuuid, 
	primary key ((title, added_year))
);

copy 
	videos_by_title_year 
from 
	'/home/labwork/exercise-3/videos_by_title_year.csv' 
with 
	header=true 
	and errfile='/home/err.txt' 
	and datetimeformat='%m/%d/%Y';
	
select * from videos_by_title_year3 where title='DataStax Cassandra Tutorials - Apache Cassandra Overview' and added_year = 2011;

select * from videos_by_title_year3 where title='DataStax Cassandra Tutorials - Apache Cassandra Overview';
 
select * from videos_by_title_year3 where added_year = 2011;