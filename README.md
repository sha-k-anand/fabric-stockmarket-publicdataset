# Header 1 - Level 1
Main header content is cool

## Header 1 - Level 2



1. Create a lakehouse
2. Shortcut the data from ADLS Gen2 using SAS keys
3. Upload the spark notebook
4. Run the spark notebook
5. Create a semantic model and set relationship between tables
5. Create reports using direct lake mode


# Shortcut ADLS Gen2


|Setting|Value|
|--|--|
|URL|https://publicdatamsdndatalake.dfs.core.windows.net/dataset|
|Connection|Create new connection|
|Connection name|publicdatastockmarket|
|Authentication Kind|Shared Access Signature (SAS)|
|SAS token|sp=rle&st=2024-02-28T15:36:24Z&se=2025-12-31T23:36:24Z&spr=https&sv=2022-11-02&sr=c&sig=D09gESF9Cd0jObLDKSLO%2F1RA1JJGXMlf1W865YDNm1o%3D|

# Shortcut ADLS Gen2


|Setting|Value|Remarks
|--|--|--|
|Shortcut Name|dataset|
|URL|https://publicdatamsdndatalake.dfs.core.windows.net|Readonly - No need to change|
|Sub Path|/dataset|

# Create a spark notebook and copy the below cells to it.


```
%%sql
DROP TABLE IF EXISTS  csv_companymaster;
CREATE TABLE  csv_companymaster
 (
Symbol        STRING,
Name          STRING,
LastSale      STRING,
NetChange     STRING,
PercentChange STRING,
MarketCap     STRING,
Country       STRING,
IPOYear       STRING,
Volume        STRING,
Sector        STRING,
Industry      STRING
) 
USING csv
OPTIONS (
path "Files/dataset/stockmarket/companymaster/*.csv",
    header "true"
)

```

```
%%sql
DROP TABLE IF EXISTS  csv_calendar;
CREATE TABLE  csv_calendar
(
SQLDate   STRING,
DateID INT,
WeekID INT,
Week STRING,
QuarterID INT,
Quarter1 STRING,
MonthID INT,
Month1 STRING,
YearMonthID INT,
YearMonth STRING,
WeekDayID INT,
WeekDay1 STRING,
Year1 INT
) 
USING csv
OPTIONS (
path "Files/dataset/stockmarket/calendar/calendar.csv",
    header "true"
)
```


```
%%sql
DROP TABLE IF EXISTS  csv_stock;
CREATE TABLE  csv_stock
 (
	TICKER       STRING,
    PER          STRING,
    DATE         INT,
    TIME         STRING,
    OPEN         DECIMAL(10,4),
    HIGH         DECIMAL(10,4),
    LOW          DECIMAL(10,4),
    CLOSE        DECIMAL(10,4),
    VOL          INT,
    OPENINT      INT
) 
USING csv
OPTIONS (
path "Files/dataset/stockmarket/marketdata/*/*",
    header "true"
)

```


```
%%pyspark
resultsDF=spark.sql("SELECT Symbol,Name,Country,IPOYear,Sector,Industry  FROM csv_companymaster")
resultsDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/companymaster")
```

```
%%pyspark
resultsDF=spark.sql("SELECT CAST(SQLDate as DATE) as SQLDate ,DateID,Week,Quarter1 as Quarter,Month1 as Month,YearMonth,WeekDay1 as WeekDay,Year1 as Year from csv_calendar")
resultsDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/dimdate")
```

```
%%pyspark
resultsDF=spark.sql("SELECT replace(TICKER,'.US','') as Ticker,DATE as DateID,OPEN as Open,HIGH as High,LOW as Low,CLOSE as Close,VOL as Vol,OPENINT as OpenInt FROM csv_stock WHERE DATE >= 20200101 AND replace(TICKER,'.US','') IN (SELECT Symbol FROM csv_companymaster)")
resultsDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/stockdata")
```

```
%%sql
DROP TABLE IF EXISTS  csv_companymaster;
DROP TABLE IF EXISTS  csv_calendar;
DROP TABLE IF EXISTS  csv_stock;
```

|Table|Row Count| Remarks|
|--|--:|--|
|companymaster|7,175||
|dimdate|20,088| date values from | 
|stockdata|5,712,950||


# Semantic Model - Table relationships

|Table 1|Table 2|Cardinality|Cross-filter direction|Make this relationship acitve|
|--|--|--|--|--|
|stockdata.DateID|dimdate.DateID|Many to one|Single|Yes|
|stockdata.Ticker|companymaster.Symbol|Many to one|Single|Yes|



ToDo
1. rename tables