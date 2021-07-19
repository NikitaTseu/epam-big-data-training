CREATE TABLE videos_by_tag_year (
	tag TEXT, 
	added_year INT, 
	video_id timeuuid, 
	added_date TIMESTAMP, 
	description TEXT, 
	title TEXT, user_id uuid, 
	PRIMARY KEY (tag, added_year, video_id)
) WITH CLUSTERING ORDER BY (added_year DESC);

copy videos_by_tag_year(
	tag, added_year, video_id, added_date, description, title, user_id
) 
from 
	'/home/labwork/exercise-4/videos_by_tag_year.csv' 
with 
	header=true;

select * from videos_by_tag_year where tag = 'snl' and added_year = 2015;

select * from videos_by_tag_year where tag = 'cql' and added_year < 2015;

select * from videos_by_tag_year where added_year < 2015;