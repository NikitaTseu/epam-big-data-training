alter table video add tags set<text>;

copy video from '/home/labwork/exercise-5/videos.csv' with header=true;

create type video_encoding (
	encoding text, 
	height int, 
	width int, 
	bit_rates set<text>
);

alter table video add video_encoding video_encoding;

copy 
	video (video_id, video_encoding) 
from 
	'/home/labwork/exercise-5/videos_encoding.csv' 
with 
	header=true;
	
select * from video limit 10;	