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

# Shortcut ADLS Gen2 dataset


|Setting|Value|Remarks
|--|--|--|
|Shortcut Name|dataset|
|URL|https://publicdatamsdndatalake.dfs.core.windows.net|Readonly - No need to change|
|Sub Path|/dataset|

# Source file statistics

|Table|Source File Count|Source File Size|Source Row Count| Source|Remarks|
|--|--:|--:|--:|--|--|
|calendar|1|1.48 mb|20,088|  Calendar data in CSV format |date values from 
|companymaster|3|< 1mb|7,175|Downloaded from https://www.nasdaq.com/market-activity/stocks/screener |
|stockmarketdata|8672|1.3 gb| 19,359,931|Downloaded from https://stooq.com/db/h/ |


# Create a spark notebook and copy the below cells to it.


```
%%sql
DROP TABLE IF EXISTS  csv_calendar;
CREATE TABLE  csv_calendar
(
SQLDate      STRING,
DateID       INT,
WeekID       INT,
Week         STRING,
QuarterID    INT,
Quarter1     STRING,
MonthID      INT,
Month1       STRING,
YearMonthID  INT,
YearMonth    STRING,
WeekDayID    INT,
WeekDay1     STRING,
Year1        INT
) 
USING csv
OPTIONS (
path "Files/dataset/stockmarket/calendar/calendar.csv",
    header "true"
)
```

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
DROP TABLE IF EXISTS  csv_stockmarketdata;
CREATE TABLE  csv_stockmarketdata
 (
    Ticker	     STRING,
    Per          STRING,
    Date         INT,
    Time         STRING,
    Open         DECIMAL(10,4),
    High         DECIMAL(10,4),
    Low          DECIMAL(10,4),
    Close        DECIMAL(10,4),
    Vol          INT,
    OpenInt      INT
) 
USING csv
OPTIONS (
path "Files/dataset/stockmarket/marketdata/*/*",
    header "true"
)

```

```
%%pyspark
resultsDF=spark.sql("SELECT CAST(SQLDate as DATE) as SQLDate ,DateID,Week,Quarter1 as Quarter,Month1 as Month,YearMonth,WeekDay1 as WeekDay,Year1 as Year FROM csv_calendar WHERE DateID >= 20000101")
resultsDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/calendar")
```



```
%%pyspark
resultsDF=spark.sql("SELECT Symbol,Name,Country,IPOYear,Sector,Industry,LEFT(Symbol,1) as SymbolStartWith  FROM csv_companymaster")
resultsDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/companymaster")
```


```
%%pyspark
resultsDF=spark.sql("SELECT REPLACE(Ticker,'.US','') as Ticker,Date as DateID,Open,High,Low,Close,Vol,OpenInt FROM csv_stockmarketdata  WHERE Date >= 20000101 AND REPLACE(Ticker,'.US','') IN (SELECT Symbol FROM csv_companymaster)")
resultsDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/stockmarketdata")
```

```
%%sql
DROP TABLE IF EXISTS  csv_calendar;
DROP TABLE IF EXISTS  csv_companymaster;
DROP TABLE IF EXISTS  csv_stockmarketdata;
```



# Semantic Model - Table relationships

|Table 1|Table 2|Cardinality|Cross-filter direction|Make this relationship acitve|
|--|--|--|--|--|
|stockdata.DateID|dimdate.DateID|Many to one|Single|Yes|
|stockdata.Ticker|companymaster.Symbol|Many to one|Single|Yes|



