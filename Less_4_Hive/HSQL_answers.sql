show databases;
show tables;

create database if not exists max_airports; 

--зачем используем use? -> для переключения на новую базу "max_airports"
use max_airports;
show tables;

drop table if exists max_airports.airport_codes;
drop table if exists max_airports.airport_codes_part;

"id","ident","type","name","latitude_deg","longitude_deg","elevation_ft","continent","iso_country","iso_region","municipality","scheduled_service","gps_code","iata_code","local_code","home_link","wikipedia_link","keywords"
6523,"00A","heliport","Total Rf Heliport",40.07080078125,-74.93360137939453,11,"NA","US","US-PA","Bensalem","no","00A",,"00A",,,
323361,"00AA","small_airport","Aero B Ranch Airport",38.704022,-101.473911,3435,"NA","US","US-KS","Leoti","no","00AA",,"00AA",,,
6524,"00AK","small_airport","Lowell Field",59.947733,-151.692524,450,"NA","US","US-AK","Anchor Point","no","00AK",,"00AK",,,
6525,"00AL","small_airport","Epps Airpark",34.86479949951172,-86.77030181884766,820,"NA","US","US-AL","Harvest","no","00AL",,"00AL",,,

create external table max_airports.airport_codes (
    id int,
    ident string,
    `type` string,
    name string,
    latitude_deg string,
    longitude_deg string,
    elevation_ft string,
    continent string,
    iso_country string,
    iso_region string,
    municipality string,
    scheduled_service string,
    gps_code string, 
    iata_code string,
    local_code string,
    home_link string,
    wikipedia_link string,
    keywords string
)
row format delimited fields terminated by ','
stored as TEXTFILE
location "/my_airports"
tblproperties ("skip.header.line.count"="1") --зачем нам эта опция? -> избавляемся от заголовка (что бы не попал в данные)
;

drop table airport_codes;

select * from airport_codes limit 10; --почему пишем лимит? -> что бы избежать переполнения оперативки выходными данными

--что делаем в этих запросах? 
select count(distinct `type`) from airport_codes; --> считаем уникальные аэропорты
select distinct `type` from airport_codes;        ---> выводим названия уникальных аэропортов 

drop table if exists max_airports.airport_codes_part; 
;
show create table airport_codes
show create table airport_codes_part
;
create table max_airports.airport_codes_part (  --> создаю таблицу с партиционированияем
    id int,
    ident string,
    name string,
    latitude_deg string,
    longitude_deg string,
    elevation_ft string,
    continent string,
    iso_country string,
    iso_region string,
    municipality string,
    scheduled_service string,
    gps_code string, 
    iata_code string,
    local_code string,
    home_link string,
    wikipedia_link string,
    keywords string
)
partitioned by (`type` string) -- указали колонку, по которой будет партиционирование
stored as TEXTFILE
location '/hive_test_loc'       -- указал папку, в которой будут лежать данные для партиционирования
;
set hive.exec.dynamic.partition.mode=nonstrict;  --> внутренняя кухня HIVE
;
insert into table airport_codes_part  --> вставляем данные в таблицу airport_codes_part
partition(`type`)
select 
    id,ident,name,latitude_deg,longitude_deg,elevation_ft,
    continent,iso_country,iso_region,municipality,scheduled_service,
    gps_code,iata_code,local_code,home_link,wikipedia_link,keywords,
    `type`
from airport_codes
tablesample (1000 rows)
;
select * from airport_codes_part tablesample (10 rows) limit 10;
select count(distinct `type`) from airport_codes_part;
select distinct `type` from airport_codes_part;

MSCK REPAIR TABLE airport_codes_part;

/*
теперь посмотри в hive_test_loc через hdfs dfs -ls/-du и скажи, что заметил и почему там всё так 
*/ --> увеличился объем данных в папке из-за того, что туда записализь данные, по которым происходит партиционирование


--что такое temporary table и когда лучше использовать? --> временные таблицы
--что будет с содержимым таблицы, если колонки, по которым партиционируем, будут стоять 
-- не последними в селекте?  --> база сломается (данне перемешаются) вылечить не получится 
create temporary table for_insert_airport_codes_part as
select 
     ident, `name`, elevation_ft, continent
    ,iso_region, municipality, gps_code, iata_code, local_code
    ,coordinates, iso_country, `type`
from student41_35.airport_codes t1
left join (
    select distinct
        `type` as type_2
    from student41_35.airport_codes_part
    ) t2 on t1.`type` = t2.type_2
where 
    t2.type_2 is null
;
select count(distinct `type`) from for_insert_airport_codes_part;
select distinct `type` from for_insert_airport_codes_part;
;
--чем insert overwrite отличается от insert into? --> insert overwrite перезаписывает текущие данные into  просто вставляет без перезаписи
insert into student41_35.airport_codes_part partition(`type`)
select 
     ident, `name`, elevation_ft, continent
    ,iso_region, municipality, gps_code, iata_code, local_code
    ,coordinates, iso_country, `type`
from for_insert_airport_codes_part t1
limit 1000
;
select count(distinct `type`) from airport_codes_part;
select distinct `type` from airport_codes_part;

/*
STREAMING
выполни в баше это и скажи, что мы тут делаем:
    seq 0 9 > col_1 && seq 10 19 > col_2
    paste -d'|' col_1 col_2 | hdfs dfs -appendToFile - test_tab/test_tab.csv
*/
;
drop table if exists my_test_tab;
;
create temporary external table my_test_tab (
    col_1 int,
    col_2 int
)
row format delimited fields terminated by '|'
stored as TEXTFILE
location "/user/student41_35/test_tab"
;
select * from my_test_tab;
;

;
--что тут произошло и как это можно использовать ещё?
select
    transform(col_1, col_2) using "awk '{print $1+$2}'" as my_sum
from my_test_tab
;

-- /home/student41_35/mapred/mapper.py 
