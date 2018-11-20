0、创建库
create database log_phone;
use log_phone;

1、编写事件的维度类和修改操作基础维度服务
2、编写udf函数

create function phone_event as 'com.yaxin.bigdata.analystic.hive.EventDimensionUdf' using jar 'hdfs://cdh:9000/phone/udfjars/yaxin_analysic-1.0.jar';
create function phone_date as 'com.yaxin.bigdata.analystic.hive.DateDimensionUdf' using jar 'hdfs://cdh:9000/phone/udfjars/yaxin_analysic-1.0.jar';
create function phone_platform as 'com.yaxin.bigdata.analystic.hive.PlatformDimensionUdf' using jar 'hdfs://cdh:9000/phone/udfjars/yaxin_analysic-1.0.jar';


创键元数据对应的临时表：
create external table if not exists phone_tmp(
ver string,
s_time string,
en string,
u_ud string,
u_mid string,
u_sd string,
c_time string,
l string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
ip String,
oid String,
`on` String,
cua String,
cut String,
pt String,
ca String,
ac String,
kv_ String,
du String,
browserName String,
browserVersion String,
osName String,
osVersion String,
country String,
province String,
city string
)
partitioned by(month string,day string)
;

load data inpath '/ods/09/19' into table phone_tmp partition(month=09,day=19);

3、创建hive表：
create external table if not exists phone(
ver string,
s_time string,
en string,
u_ud string,
u_mid string,
u_sd string,
c_time string,
l string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
ip String,
oid String,
`on` String,
cua String,
cut String,
pt String,
ca String,
ac String,
kv_ String,
du String,
browserName String,
browserVersion String,
osName String,
osVersion String,
country String,
province String,
city string
)
partitioned by(month string,day string)
stored as orc
;


set hive.exec.local.mode=true;
from phone_tmp
insert into phone partition(month=09,day=19)
select
ver,
s_time,
en,
u_ud,
u_mid,
u_sd,
c_time,
l,
b_iev,
b_rst,
p_url,
p_ref,
tt,
pl,
ip,
oid,
`on`,
cua,
cut,
pt,
ca,
ac,
kv_,
du,
browserName,
browserVersion,
osName,
osVersion,
country,
province,
city
where month = 9
and day = 19
;

在hive中创建和mysql最终结果便一样的临时表：
CREATE TABLE if not exists `stats_event` (
  `platform_dimension_id` int,
  `date_dimension_id` int,
  `event_dimension_id` int,
  `times` int,
  `created` String
)
;


语句：
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
from(
select
from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
p.pl as pl,
p.ca as ca,
p.ac as ac,
count(*) as ct
from phone p
where p.month = 9
and p.day = 19
and en = 'e_l'
group by from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.ca,p.ac
) as tmp
insert overwrite table stats_event
select phone_platform(pl),phone_date(dt),phone_event(ca,ac),ct,dt
;

扩展维度：
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
with tmp as(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.ca as ca,
l.ac as ac
from phone l
where month = 9
and day = 19
and l.en = 'e_l'
and l.s_time <> 'null'
)
from (
select dt as dt,pl as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,pl,ca,ac union all
select dt as dt,pl as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,pl,ca union all
select dt as dt,'all' as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select dt as dt,'all' as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,ca
) as tmp1
insert overwrite table stats_event
select phone_date(dt),phone_platform(pl),phone_event(ca,ac),sum(ct),'2018-09-19'
group by pl,dt,ca,ac
;




3	1	1
3	1	3
3	3	1
3	3	3

sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password 911208 -m 1 \
--table stats_event --export-dir hdfs://hadoop01:9000/user/hive/warehouse/log_phone.db/stats_event/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,event_dimension_id \
;




1	3	订单事件|订单产生	1	2018-09-19
1	3	订单事件|订单支付	1	2018-09-19
1	3	点击|赞	200	2018-09-19