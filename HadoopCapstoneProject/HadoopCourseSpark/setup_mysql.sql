/*
mysql -h sandbox-hdp.hortonworks.com -u root -p < sql/setup_mysql.sql
*/
create database if not exists big_data;
use big_data;
/*
drop table if exists top_purchased_categories;
create table top_purchased_categories (
    category text,
    purchased_products_num int
);

drop table if exists top_purchased_products_per_category;
create table top_purchased_products_per_category (
  product_category text,
  product_name text,
  purchase_count int
);

drop table if exists top_spending_countries;
create table top_spending_countries (
  country_name text,
  money_spent int
);*/

drop table if exists spark_sql_top_purchased_categories;
create table spark_sql_top_purchased_categories (
    category text,
    purchased_products_num int
);

drop table if exists spark_sql_top_purchased_products_per_category;
create table spark_sql_top_purchased_products_per_category (
  product_category text,
  product_name text,
  purchase_count int
);

drop table if exists spark_sql_top_spending_countries;
create table spark_sql_top_spending_countries (
  country_name text,
  money_spent int
);

drop table if exists spark_rdd_top_purchased_categories;
create table spark_sql_top_purchased_categories (
    category text,
    purchased_products_num int
);

drop table if exists spark_rdd_top_purchased_products_per_category;
create table spark_sql_top_purchased_products_per_category (
  product_category text,
  product_name text,
  purchase_count int
);

drop table if exists spark_rdd_top_spending_countries;
create table spark_sql_top_spending_countries (
  country_name text,
  money_spent int
);
