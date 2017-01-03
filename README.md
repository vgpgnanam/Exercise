# Exercise

#01
Copy all the directories from retail_db under /tmp to HDFS (user HDFS home directory - /user/your_lab_id)

Hint: Use copyFromLocal or put to copy the files

Provide the output for below

Please list the files you have copied (using ls command)
Also get the results about files, blocks and locations on all the files that are copied.

#01 Answer
hadoop fs -put /tmp/retail_db /user/gnanaprakasam/retail_db
hadoop fs -ls /user/gnanaprakasam/retail_db
hdfs fsck /user/gnanaprakasam/retail_db/categories -files -blocks -locations


#03
create table orders (
order_id int,
order_date string,
order_customer_id int,
order_status string)
row format delimited fields terminated by ',';

describe formatted orders;

LOAD DATA LOCAL INPATH '/tmp/retail_db/orders' OVERWRITE INTO TABLE orders;
Problem:

Make sure you have database with your username and it is selected
Create 6 tables, at least one as external table
For all managed tables, load data from your HDFS location to tables under your database
Please provide the following information

Run show tables and paste the output
Output of metadata for 1 table (describe formatted)
Script to load data
Run query with limit 10 on all the tables - and paste for one of the tables

#03 - Answers

# Managed Table creation for Categories

# Check table is NOT present in Hive
hive> show tables;
OK

# Create the table in Hive
hive> create table categories(
    > category_id int,
    > category_department_id int,
    > category_name string)
    > row format delimited fields terminated by ',';
OK
Time taken: 0.523 seconds

# Check the table is created in Hive
hive> show tables;
OK
categories

# Check the data present in HDFS before load
[gnanaprakasam@gw01 ~]$ hadoop fs -ls /user/gnanaprakasam/retail_db/categories
Found 1 items
-rw-r--r--   3 gnanaprakasam hadoop       1029 2016-12-14 13:18 /user/gnanaprakasam/retail_db/categories/part-00000

# Check is there any data in before load
hive> select * from categories;
OK
Time taken: 0.429 seconds

# It will replace the data in Hive table also delte the source data from HDFS (Note:- When we use local it won't delete the source data)
load data inpath '/user/gnanaprakasam/retail_db/categories' overwrite into table categories;

# See the data loaded properly
hive> select * from categories limit 10;
OK
1       2       Football
2       2       Soccer
3       2       Baseball & Softball
4       2       Basketball
5       2       Lacrosse
6       2       Tennis & Racquet
7       2       Hockey
8       2       More Sports
9       3       Cardio Equipment
10      3       Strength Training
Time taken: 0.146 seconds, Fetched: 10 row(s)

# Directory deleted from HDFS (Data is MOVED from HDFS to Hive table)
[gnanaprakasam@gw01 ~]$ hadoop fs -ls /user/gnanaprakasam/retail_db/categories
ls: `/user/gnanaprakasam/retail_db/categories': No such file or directory

# Below meta data in categories shows which is MANGED_TABLE, "," as delimiter & Tex is input and output format

hive> describe formatted categories;
OK
# col_name              data_type               comment             
                 
category_id             int                                         
category_department_id  int                                         
category_name           string                                      
                 
# Detailed Table Information             
Database:               gnanaprakasam            
Owner:                  gnanaprakasam            
CreateTime:             Wed Dec 14 19:06:46 EST 2016     
LastAccessTime:         UNKNOWN                  
Protect Mode:           None                     
Retention:              0                        
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/categories   
Table Type:             MANAGED_TABLE            
Table Parameters:                
        numFiles                1                   
        numRows                 0                   
        rawDataSize             0                   
        totalSize               1029                
        transient_lastDdlTime   1481760562          
                 
# Storage Information            
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
Compressed:             No                       
Num Buckets:            -1                       
Bucket Columns:         []                       
Sort Columns:           []                       
Storage Desc Params:             
        field.delim             ,                   
        serialization.format    ,                   
Time taken: 0.352 seconds, Fetched: 33 row(s)

# Since the record is deleted from HDFS, checking the record count from original source (which is copied from local to HDFS).  It shows the total in source and Hive table matches(It successfully loaded everything)
[gnanaprakasam@gw01 ~]$ cat /tmp/retail_db/categories/part*|wc -l
58

hive> select count(1) from categories;
Query ID = gnanaprakasam_20161214192841_8be90182-fdfa-42e8-bfd9-2617c485ce59
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
...
MapReduce Total cumulative CPU time: 4 seconds 590 msec
Ended Job = job_1480307771710_2832
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.59 sec   HDFS Read: 8933 HDFS Write: 3 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 590 msec
OK
58



# External table creation for departments table

# delete the table if it's already exist
hive> drop table departments;
OK
Time taken: 0.463 seconds

# Create the table in Hive
hive> create external table departments(
    > department_id int,
    > department_name string)
    > row format delimited fields terminated by ',';
OK
Time taken: 0.449 seconds

# Check the table is created in Hive

hive> show tables;
OK
categories
departments

# Check the data present in HDFS before load
[gnanaprakasam@gw01 ~]$ hadoop fs -ls /user/gnanaprakasam/retail_db/departments
Found 1 items
-rw-r--r--   3 gnanaprakasam hadoop         60 2016-12-14 13:18 /user/gnanaprakasam/retail_db/departments/part-00000
[gnanaprakasam@gw01 ~]$

# Check is there any data in before load
hive> select * from departments;
OK
Time taken: 0.517 seconds

# checking the record count from source to verify later all the records are loaded successfully.

[gnanaprakasam@gw01 ~]$ cat /tmp/retail_db/departments/part*|wc -l
6
[gnanaprakasam@gw01 ~]$ hadoop fs -cat /user/gnanaprakasam/retail_db/departments/part*|wc -l
6

# It will replace the data in Hive table and source records still present (Note:- When we use HDFS path it will delete the source data)
load data local inpath '/tmp/retail_db/departments' into table departments;

# See the data loaded properly
hive> select * from departments limit 10;
OK
2       Fitness
3       Footwear
4       Apparel
5       Golf
6       Outdoors
7       Fan Shop
Time taken: 0.287 seconds, Fetched: 6 row(s)

# Source data remain same in local (Data is COPIED from local to Hive table), here we didn't touched the data in HDFS.
[gnanaprakasam@gw01 ~]$ cat /tmp/retail_db/departments/part*|wc -l
6
[gnanaprakasam@gw01 ~]$ hadoop fs -cat /user/gnanaprakasam/retail_db/departments/part*|wc -l
6


# Below meta data in departments shows which is MANGED_TABLE, "," as delimiter & Tex is input and output format

hive> describe formatted departments;
OK
# col_name              data_type               comment             
                 
department_id           int                                         
department_name         string                                      
                 
# Detailed Table Information             
Database:               gnanaprakasam            
Owner:                  gnanaprakasam            
CreateTime:             Wed Dec 14 20:41:01 EST 2016     
LastAccessTime:         UNKNOWN                  
Protect Mode:           None                     
Retention:              0                        
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/departments  
Table Type:             EXTERNAL_TABLE           
Table Parameters:                
        EXTERNAL                TRUE                
        numFiles                1                   
        numRows                 0                   
        rawDataSize             0                   
        totalSize               60                  
        transient_lastDdlTime   1481770860          
                 
# Storage Information            
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
Compressed:             No                       
Num Buckets:            -1                       
Bucket Columns:         []                       
Sort Columns:           []                       
Storage Desc Params:             
        field.delim             ,                   
        serialization.format    ,                   
Time taken: 0.372 seconds, Fetched: 33 row(s)

hive> select count(1) from departments;
Query ID = gnanaprakasam_20161214220557_ff341b89-bc54-409d-bd7a-8a0fc7fbea8e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
...
MapReduce Total cumulative CPU time: 4 seconds 820 msec
Ended Job = job_1480307771710_2844
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.82 sec   HDFS Read: 7761 HDFS Write: 2 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 820 msec
OK
6


# Exercise 04 - Partition order_items with order_month

CREATE TABLE orders_dg (
order_id int, 
order_date timestamp, 
order_customer_id int, 
order_status string)
PARTITIONED BY (order_month string)
STORED AS avro
;

alter table orders_dg add partition (order_month = '2014-01');

insert into table orders_dg
partition (order_month = '2014-01')
select * from orders where substr(order_date, 1, 7) = '2014-01';

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table orders_dg partition (order_month)
select order_id, order_date, order_customer_id, order_status,
substr(order_date, 1, 7) order_month
from orders where substr(order_date, 1, 7) != '2014-01';
Problem:

# Exercise 04 Answers

Create partitioned order_items table by order_month
You need to join orders and order_items on order_id and order_item_order_id
You have to provide

Output of describe formatted
Output of dfs -ls command
Output of select * from your_table_name limit 10;

# Exercise 04 Answer

create table order_items_partition (
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity tinyint,
order_item_subtotal float,
order_item_product_price float)
partitioned by (order_month string)
;

describe formatted order_items_partition;

insert into table order_items_partition partition(order_month)
select oi.order_item_id, oi.order_item_order_id, oi.order_item_product_id, oi.order_item_quantity,
oi.order_item_subtotal, oi.order_item_product_price, substr(o.order_date, 1, 7) order_month from orders o join
order_items oi where o.order_id = oi.order_item_order_id;

hive> select * from order_items_partition limit 10;
OK
1       1       957     1       299.98  299.98  2013-07
2       2       1073    1       199.99  199.99  2013-07
3       2       502     5       250.0   50.0    2013-07
4       2       403     1       129.99  129.99  2013-07
5       4       897     2       49.98   24.99   2013-07
6       4       365     5       299.95  59.99   2013-07
7       4       502     3       150.0   50.0    2013-07
8       4       1014    4       199.92  49.98   2013-07
9       5       957     1       299.98  299.98  2013-07
10      5       365     5       299.95  59.99   2013-07

hive> dfs -ls /apps/hive/warehouse/gnanaprakasam.db/order_items_partition;
Found 13 items
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2013-07
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2013-08
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2013-09
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2013-10
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2013-11
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2013-12
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-01
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-02
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-03
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-04
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-05
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-06
drwxrwxrwx   - gnanaprakasam hdfs          0 2016-12-14 23:03 /apps/hive/warehouse/gnanaprakasam.db/order_items_partition/order_month=2014-07
hive>

# Exercise 05

Problem:

Get only completed orders (hint: order_state = 'COMPLETE')
Get revenue for each department per day
Insert output into a new table
Hint: Join all tables except customers

Please provide following as output:

Number of records
Sample output of 10 records (It should have order_date, department_name and revenue for each department)

# Exercise 05 Answers

select o.order_date, d.department_name, sum(oi.order_item_subtotal) revenue_per_day
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join products p on oi.order_item_product_id = p.product_id
join categories c on p.product_category_id = c.category_id
join departments d on c.category_department_id = d.department_id
where o.order_status = 'COMPLETE'
group by o.order_date, d.department_name;

# Total number of records
Time taken: 28.537 seconds, Fetched: 2116 row(s)

hive> create table revenue_per_day_department (
    > order_date string,
    > order_department string,
    > order_subtotal float)
    > ;
OK
Time taken: 0.735 seconds
hive> select * from revenue_per_day_department;
OK
Time taken: 0.303 seconds
hive> 


hive> describe formatted revenue_per_day_department;

insert into table revenue_per_day_department 
select o.order_date, d.department_name, sum(oi.order_item_subtotal) revenue_per_day
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join products p on oi.order_item_product_id = p.product_id
join categories c on p.product_category_id = c.category_id
join departments d on c.category_department_id = d.department_id
where o.order_status = 'COMPLETE'
group by o.order_date, d.department_name;

hive> select * from revenue_per_day_department limit 10;
OK
2013-07-25 00:00:00.0   Apparel 3279.57
2013-07-25 00:00:00.0   Fan Shop        9798.69
2013-07-25 00:00:00.0   Fitness 394.93
2013-07-25 00:00:00.0   Footwear        3899.61
2013-07-25 00:00:00.0   Golf    2029.72
2013-07-25 00:00:00.0   Outdoors        627.8
2013-07-26 00:00:00.0   Apparel 8828.75
2013-07-26 00:00:00.0   Fan Shop        20847.68
2013-07-26 00:00:00.0   Fitness 183.98
2013-07-26 00:00:00.0   Footwear        5129.42
Time taken: 0.168 seconds, Fetched: 10 row(s)
hive>

hive> select count(1) from revenue_per_day_department;
OK
2116
Time taken: 0.196 seconds, Fetched: 1 row(s)
hive>

Exercise 06

Reference Documentation - https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html

Problem:

Create HDFS directory with name retail_data
Import all tables using avro file format, compression and number of mappers as 2
Please use this hint for avro format - -Dmapreduce.job.user.classpath.first=true
Import orders table to hive database of yours (text file) - Using sqoop import with --hive-import and delimiter '|' - use table name orders_sqooped
Export the output of revenue per day per department query to mysql
Create table in mysql logging in as retail_dba
Database - retail_export
Username - retail_dba
Please provide the following
* Output of hadoop fs -ls -R /user/YOUR_USER_NAME/retail_data
* Output of describe formatted orders_sqooped
* Output of select * from retail_export.YOUR_TABLE_NAME limit 10

sqoop import-all-tables -Dmapreduce.job.user.classpath.first=true \
  --connect "jdbc:mysql://nn01.itversity.com:3306/retail_db" \
  --username=retail_dba \
  --password=itversity \
  --as-avrodatafile \
  --num-mappers 2 \
  --compress \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --warehouse-dir=/user/gnanaprakasam/sqoop_import_New


sqoop import \
  --connect "jdbc:mysql://nn01.itversity.com:3306/retail_db" \
  --username=retail_dba \
  --password=itversity \
  --as-textfile \
  --hive-import \
  --create-hive-table \
  --hive-overwrite \
  --table orders \
  --target-dir /apps/hive/warehouse/gnanaprakaasam.db/orders_sqooped \
  --hive-table orders_sqooped \
  --hive-database gnanaprakasam \
  --fields-terminated-by '|' \
  --lines-terminated-by '\n'

hive> describe formatted orders_sqooped;
OK
# col_name              data_type               comment             
                 
order_id                int                                         
order_date              string                                      
order_customer_id       int                                         
order_status            string                                      
                 
# Detailed Table Information             
Database:               gnanaprakasam            
Owner:                  gnanaprakasam            
CreateTime:             Thu Dec 15 23:34:18 EST 2016     
LastAccessTime:         UNKNOWN                  
Protect Mode:           None                     
Retention:              0                        
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/orders_sqooped       
Table Type:             MANAGED_TABLE            
Table Parameters:                
        comment                 Imported by sqoop on 2016/12/15 23:34:15
        numFiles                4                   
        numRows                 0                   
        rawDataSize             0                   
        totalSize               2999944             
        transient_lastDdlTime   1481862859          
                 
# Storage Information            
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
Compressed:             No                       
Num Buckets:            -1                       
Bucket Columns:         []                       
Sort Columns:           []                       
Storage Desc Params:             
        field.delim             |                   
        line.delim              \n                  
        serialization.format    |                   
Time taken: 0.363 seconds, Fetched: 36 row(s)

hive> select * from orders_sqooped limit 10;
OK
1       2013-07-25 00:00:00.0   11599   CLOSED
2       2013-07-25 00:00:00.0   256     PENDING_PAYMENT
3       2013-07-25 00:00:00.0   12111   COMPLETE
4       2013-07-25 00:00:00.0   8827    CLOSED
5       2013-07-25 00:00:00.0   11318   COMPLETE
6       2013-07-25 00:00:00.0   7130    COMPLETE
7       2013-07-25 00:00:00.0   4530    COMPLETE
8       2013-07-25 00:00:00.0   2911    PROCESSING
9       2013-07-25 00:00:00.0   5657    PENDING_PAYMENT
10      2013-07-25 00:00:00.0   5648    PENDING_PAYMENT
Time taken: 0.194 seconds, Fetched: 10 row(s)

[gnanaprakasam@gw01 ~]$ mysql -u retail_dba -h nn01.itversity.com -p

mysql> show databases;

mysql> use retail_export;

mysql> select * from dep_rev_per_day_farhan  limit 10;

mysql> create table revenue_per_day_dep_gnana select * from dep_rev_per_day_farhan where 1=2;
Query OK, 0 rows affected (0.17 sec)
Records: 0  Duplicates: 0  Warnings: 0

mysql> select * from revenue_per_day_dep_gnana;
Empty set (0.00 sec)

sqoop export \
  --connect "jdbc:mysql://nn01.itversity.com:3306/retail_export" \
  --username=retail_dba \
  --password=itversity \
  --table revenue_per_day_dep_gnana \
  --input-fields-terminated-by '\001' \
  --export-dir /apps/hive/warehouse/gnanaprakasam.db/revenue_per_day_department \
  --num-mappers 1

  mysql> select * from revenue_per_day_dep_gnana limit 10;
+-----------------------+-----------------+-----------------+
| order_date            | department_name | revenue_per_day |
+-----------------------+-----------------+-----------------+
| 2013-07-25 00:00:00.0 | Apparel         |         3279.57 |
| 2013-07-25 00:00:00.0 | Fan Shop        |         9798.69 |
| 2013-07-25 00:00:00.0 | Fitness         |          394.93 |
| 2013-07-25 00:00:00.0 | Footwear        |         3899.61 |
| 2013-07-25 00:00:00.0 | Golf            |         2029.72 |
| 2013-07-25 00:00:00.0 | Outdoors        |           627.8 |
| 2013-07-26 00:00:00.0 | Apparel         |         8828.75 |
| 2013-07-26 00:00:00.0 | Fan Shop        |        20847.68 |
| 2013-07-26 00:00:00.0 | Fitness         |          183.98 |
| 2013-07-26 00:00:00.0 | Footwear        |         5129.42 |
+-----------------------+-----------------+-----------------+
10 rows in set (0.00 sec)
