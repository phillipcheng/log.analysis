#!/bin/bash
for j in {1..41..5}
do
	for i in {0..8000..500}
	do
		num=`vsql -d cmslab -U dbadmin -w password -c "select count(*) as mintotal, timestamp_round(dt,'MI') as min from lsl_csv where primaryid not in (select primaryid from lsl_csv where timestamp_round(dt,'MI') in (select mindt from lsl_minute order by total desc limit $j) group by primaryid order by count(primaryid) desc limit $i) group by min order by mintotal desc limit 2" | sed -n 3p | cut -d'|' -f1`
		printf "%d,%d,%s\n" $j $i $num
	done
done
