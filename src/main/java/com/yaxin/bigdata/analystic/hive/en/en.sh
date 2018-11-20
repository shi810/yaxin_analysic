#!/bin/bash

##### ./en.sh -d 2018-09-19

dt=''
#循环运行时所带的参数
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
then
shift
dt=$1
fi
shift
done

month=
day=
#判断日期是否合法和正常
if [ ${#dt} = 10 ]
then
echo "dt:$dt"
else
dt=`date -d "1 days ago" "+%Y-%m-%d"`
fi

#计算month和day
month=`date -d "$dt" "+%m"`
day=`date -d "$dt" "+%d"`
echo "running date is:$dt,month is:$month,day is:$day"
echo "running hive SQL statment..."

#run hive hql

hive --database log_phone -e "
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
from(
select
from_unixtime(cast(p.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
p.pl as pl,
p.ca as ca,
p.ac as ac,
count(*) as ct
from phone p
where p.month = "${month}"
and p.day = "${day}"
and en = 'e_e'
group by from_unixtime(cast(p.s_time/1000 as bigint),'yyyy-MM-dd'),p.pl,p.ca,p.ac
) as tmp
insert overwrite table stats_event
select phone_platform(pl),phone_date(dt),phone_event(ca,ac),ct,dt
"
;


#run sqoop statment
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root \
--table stats_event --export-dir /hive/log_phone.db/stats_event/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,event_dimension_id \
;

echo "the event job is fininshed."