drop table if exists max_airports.airport_codes_part_2;
show create table airport_codes_part_2

select * from airport_codes_part_2 limit 10

create table max_airports.airport_codes_part_2 (  --> создаю таблицу с партиционированияем
    id int,
    ident string,
    name string,
    latitude_deg string,
    longitude_deg string,
    elevation_ft string,
    continent string,
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
partitioned by (`type` string, `iso_country` string) -- указали колонку, по которой будет партиционирование
stored as TEXTFILE
location '/hive_test_type'    

;
set hive.exec.dynamic.partition.mode=nonstrict;  --> внутренняя кухня HIVE
;
insert into table max_airports.airport_codes_part_2  --> вставляем данные в таблицу airport_codes_part
partition(`type`, `iso_country`)
select 
    id,ident,name,latitude_deg,longitude_deg,elevation_ft,
    continent,iso_region,municipality,scheduled_service,
    gps_code,iata_code,local_code,home_link,wikipedia_link,keywords,
    `type`, `iso_country`
from airport_codes_part_2
tablesample (1000 rows)
;

select 
    id,ident,name,latitude_deg,longitude_deg,elevation_ft,
    continent,iso_country,iso_region,municipality,scheduled_service,
    gps_code,iata_code,local_code,home_link,wikipedia_link,keywords,
    `type`, `iso_country`
from airport_codes_part_2  limit 10;
