数据抽取(抽取该模块中需要的字段即可)：
load data inpath '/ods/month=08/day=18' into table logs partition(month=08,day=18);


考虑是否需要udf函数(kpi)：


创建最终结果表：
CREATE TABLE IF NOT EXISTS `stats_view_depth` (
  `platform_dimension_id` int,
  `data_dimension_id` int,
  `kpi_dimension_id` int,
  `pv1` int,
  `pv2` int,
  `pv3` int,
  `pv4` int,
  `pv5_10` int,
  `pv10_30` int,
  `pv30_60` int,
  `pv60pluss` int,
  `created` string
  );

创建临时表：
CREATE TABLE IF NOT EXISTS `stats_view_depth_tmp` (
 dt string,
 pl string,
 col string,
 ct int
);

2018-09-19 website pv1 10
2018-09-19 website pv2 200

3   1   2   10  200 0   0   0   0   2018-09-19

3   1   2   10  0 0   0   0   0   2018-09-19
3   1   2   0  200 0   0   0   0   2018-09-19
3   1   2   10  0 0   0   0   0   2018-09-19

sql语句：


set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
from(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.u_ud as uid,
(case
when count(l.p_url) = 1 then "pv1"
when count(l.p_url) = 2 then "pv2"
when count(l.p_url) = 3 then "pv3"
when count(l.p_url) = 4 then "pv4"
when count(l.p_url) < 10 then "pv5_10"
when count(l.p_url) < 30 then "pv10_30"
when count(l.p_url) < 60 then "pv30_60"
else "pv60pluss"
end) as pv
from phone l
where month = 09
and day = 19
and l.p_url <> 'null'
and l.pl is not null
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,u_ud
) as tmp
insert overwrite table stats_view_depth_tmp
select dt,pl,pv,count(distinct uid) as ct
where uid is not null
group by dt,pl,pv
;


set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
with tmp as(
select dt,pl as pl,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv1' union all
select dt,pl as pl,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv2' union all
select dt,pl as pl,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv3' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv4' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv5_10' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv10_30' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv30_60' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from stats_view_depth_tmp where col = 'pv60pluss'
)
from tmp
insert overwrite table stats_view_depth
select phone_date(dt),phone_platform(pl),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),dt
group by dt,pl
;



编写sqoop语句：
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
 --username root --password root \
 --table stats_view_depth --export-dir /hive/log_phone.db/stats_view_depth/* \
 --input-fields-terminated-by "\\01" --update-mode allowinsert \
 --update-key date_dimension_id,platform_dimension_id,kpi_dimension_id \
 ;



用户角度下的浏览深度：
2018-08-17 website http://localhost:8080/index.html 123
2018-08-17 website http://localhost:8080/index.html 123
2018-08-17 website http://localhost:8080/index.html 123

2018-08-17 website http://localhost:8080/index1.html 345
2018-08-17 website http://localhost:8080/index.html 345

先统计pv的值：分组 日期，pl,用户

2018-08-18	website	2A6FB951-F4FC-4886-87C0-E9C9D47D2C5C	pv1
2018-08-18	website	D4289356-5BC9-47C4-8F7D-F16022833E7E	pv1

2018-08-18	website	pv1	2
2018-08-18	website	pv10_30	90

group by


2018-08-17 website 1 pv1
2018-08-17 website 1 pv1
2018-08-17 website 3 pv3

将统计的pv的值和对应的pv1，pv2...等存储到临时表

dt string,
pl string,
col string,
value ''

查询临时表并扩维度:
