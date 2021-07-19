create keyspace killrvideo
with replication = {
	'class': 'SimpleStrategy', 'replication_factor': 3
};

use killrvideo;

create table video(
	video_id timeuuid, 
	added_date timestamp, 
	description text, 
	title text, 
	user_id uuid, 
	primary key (video_id)
);

copy video from '/home/labwork/exercise-2/videos.csv' with header=true;

select * from video limit 10;

select count(*) from video;

select * from video where video_id = 6c4cffb9-0dc4-1d59-af24-c960b5fc3652;

truncate video;

select * from video limit 1;