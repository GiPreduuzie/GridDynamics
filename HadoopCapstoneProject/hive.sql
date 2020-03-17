/*-------------
LOAD EVENTS TO HIVE
*/
drop table if exists events;
create external table events
  ( product_name string,
    product_category string,
    product_price double,
    tstmp string,
    client_ip string
  )
  partitioned by (purchase_date string)
  row format delimited
  fields terminated by ','
  stored as textfile
  location '/user/root/flume';


alter table events add partition(purchase_date='2020-03-06') location '/user/root/flume/2020/03/06';
alter table events add partition(purchase_date='2020-03-07') location '/user/root/flume/2020/03/07';
alter table events add partition(purchase_date='2020-03-08') location '/user/root/flume/2020/03/08';
alter table events add partition(purchase_date='2020-03-09') location '/user/root/flume/2020/03/09';
alter table events add partition(purchase_date='2020-03-10') location '/user/root/flume/2020/03/10';
alter table events add partition(purchase_date='2020-03-11') location '/user/root/flume/2020/03/11';
alter table events add partition(purchase_date='2020-03-12') location '/user/root/flume/2020/03/12';
alter table events add partition(purchase_date='2020-03-13') location '/user/root/flume/2020/03/13';

/*-------------
IMPORT GEODATA
*/
drop table if exists geo_ip;
create table geo_ip(
    network string,
    geoname_id int,
    registered_country_geoname_id int,
    represented_country_geoname_id int,
    is_anonymous_proxy boolean,
    is_satellite_provider boolean,
    postal_code string,
    latitude float,
    longitude float,
    accuracy_radius float
)
row format delimited
fields terminated by ','
stored as textfile;

LOAD DATA INPATH 'hdfs://sandbox-hdp.hortonworks.com:8020/user/root/import/geo_ips.csv' OVERWRITE INTO TABLE geo_ip;

drop table if exists city_locations;
create table if not exists city_locations(
geoname_id int,
locale_code string,
continent_code string,
continent_name string,
country_iso_code string,
country_name string,
subdivision_1_iso_code string,
subdivision_2_name string,
city_name string,
metro_code string,
time_zone string,
is_in_european_union boolean)
row format delimited
fields terminated by ','
stored as textfile
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://sandbox-hdp.hortonworks.com:8020/user/root/import/geo_locations.csv' OVERWRITE INTO TABLE city_locations;

/*-------------
TOP 10 MOST PURCHASED CATEGORIES
*/

insert overwrite directory '/user/root/export/top_purchased_categories'
row format delimited fields terminated by ','
stored as textfile
select product_category, count(product_name) as purchased_products_num
from events
group by product_category
order by purchased_products_num desc
limit 10;

/*-------------
TOP 10 MOST FREQUENTLY PURCHASED PRODUCTS IN EACH CATEGORY
*/

drop table if exists top_purchased_products_per_category;
create table top_purchased_products_per_category (
  product_category string,
  product_name string,
  purchase_count int
)
stored as orc;

insert into table top_purchased_products_per_category
select product_category, product_name, purchase_count from (
    with a as (
        select distinct product_category, product_name,
        count() over (
            partition by product_category, product_name
        ) as purchase_count
        from events
        order by product_category asc, purchase_count desc)
    select product_category, product_name, purchase_count,
    row_number() over (
        partition by product_category
        order by purchase_count desc, product_name asc
    ) as row_number
    from a
) as b
where row_number <= 10;

insert overwrite directory '/user/root/export/top_purchased_products_per_category'
row format delimited fields terminated by ','
stored as textfile
select *
from top_purchased_products_per_category;

/*-------------
TOP 10 COUNTRIES WITH THE HIGHEST MONEY SPENDING
*/

create temporary function get_ip as 'com.gridu.hive.udf.IpExtractor' USING JAR 'hdfs://sandbox-hdp.hortonworks.com:8020/user/root/hive/hive_udf.jar';
create temporary function get_network_size as 'com.gridu.hive.udf.NetworkSizeExtractor' USING JAR 'hdfs://sandbox-hdp.hortonworks.com:8020/user/root/hive/hive_udf.jar';
create temporary function mask_ip as 'com.gridu.hive.udf.IpMasker' USING JAR 'hdfs://sandbox-hdp.hortonworks.com:8020/user/root/hive/hive_udf.jar';

drop table if exists country_network;
create table country_network (
    network_size int,
    network int,
    country_name string
)
stored as orc;

insert into country_network
select network_size, network, country_name
from
(select get_network_size(network) as network_size, get_ip(network) as network, country_name
from geo_ip join city_locations on geo_ip.geoname_id == city_locations.geoname_id
where country_name != "") as a;

drop table if exists price_with_countries;
create table price_with_countries (
    country_name string,
    money_spent double
)
stored as orc;

insert into table price_with_countries
select country_name, product_price as money_spent
from country_network join (
with s as (select distinct network_size from country_network order by network_size)
select network_size, client_ip, mask_ip(client_ip, network_size) as network, product_price
from s join events) as e
on e.network_size = country_network.network_size and country_network.network = e.network
order by country_name;

insert overwrite directory '/user/root/export/top_spending_countries'
row format delimited fields terminated by ','
stored as textfile
select country_name, sum(money_spent) as money_spent
from price_with_countries
group by country_name
order by money_spent desc
limit 10;
