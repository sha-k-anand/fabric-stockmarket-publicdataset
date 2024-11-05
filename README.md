

## Step 1. Create  a workspace
    Create a new workspace in fabric
## Step 2. Create  a Lakehouse
  Navigate to the newly created workspace and create a new lakehouse in it.
  This should enable fabric trial and assign the newly created workspace to the trial capacity.

## Step 3. Shortcut the data from ADLS Gen2 using SAS keys
Navigate to the lakehouse and click on the files section and create a new shortcut


<details>
  

### Shortcut ADLS Gen2


|Setting|Value|
|--|--|
|URL|```https://publicdatamsdndatalake.dfs.core.windows.net/dataset```|
|Connection|Create new connection|
|Connection name|```publicdatastockmarket```|
|Authentication Kind|Shared Access Signature (SAS)|
|SAS token|```sp=rle&st=2024-02-28T15:36:24Z&se=2025-12-31T23:36:24Z&spr=https&sv=2022-11-02&sr=c&sig=D09gESF9Cd0jObLDKSLO%2F1RA1JJGXMlf1W865YDNm1o%3D```|

### Shortcut ADLS Gen2 dataset

Add a check box - remove below section
Navigate and browse shortcuts to view files



|Setting|Value|
|--|--|
|Shortcut Name|dataset|
|URL|```https://publicdatamsdndatalake.dfs.core.windows.net```|
|Sub Path|/dataset|

### Source file statistics

|Table|Source File Count|Source File Size|Source Row Count| Source|
|--|--:|--:|--:|--|
|calendar|1|1.48 mb|20,088|  Calendar data in CSV format |
|companymaster|3|< 1mb|7,175|Downloaded from https://www.nasdaq.com/market-activity/stocks/screener |
|stockmarketdata|8672|1.3 gb| 19,359,931|Downloaded from https://stooq.com/db/h/  Daily US ASCII - Until July 19 2024|

</details>

## Step 3. Create a spark notebook and copy the below cells to it.
<details>

  <summary>View notebook code</summary>

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
USING csv OPTIONS (path "Files/dataset/stockmarket/calendar/calendar.csv", header "true");

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
USING csv OPTIONS (path "Files/dataset/stockmarket/companymaster/*.csv",    header "true");

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
USING csv OPTIONS (path "Files/dataset/stockmarket/stockmarketdata/*/*",    header "true")

```

```
%%pyspark
resultsDFcalendar=spark.sql("SELECT CAST(SQLDate as DATE) as SQLDate ,DateID,WeekID,Week,QuarterID,Quarter1 as Quarter,MonthID,Month1 as Month,YearMonthID,YearMonth,WeekDayID,WeekDay1 as WeekDay,Year1 as Year FROM csv_calendar WHERE DateID >= 20000101")
resultsDFcalendar.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/calendar")

resultsDFcompanymaster=spark.sql("SELECT Symbol,Name,Country,IPOYear,Sector,Industry,LEFT(Symbol,1) as SymbolStartWith  FROM csv_companymaster")
resultsDFcompanymaster.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/companymaster")

resultsDFstockmarketdata=spark.sql("SELECT REPLACE(Ticker,'.US','') as Ticker,Date as DateID,Open,High,Low,Close,Vol,OpenInt FROM csv_stockmarketdata  WHERE Date >= 20000101 AND REPLACE(Ticker,'.US','') IN (SELECT Symbol FROM csv_companymaster)")
resultsDFstockmarketdata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/stockmarketdata")
```

```
%%sql
DROP TABLE IF EXISTS  csv_calendar;
DROP TABLE IF EXISTS  csv_companymaster;
DROP TABLE IF EXISTS  csv_stockmarketdata;
```
</details>


## Step 4. Create a new customer Semantic Model and set Table relationships

Please avoid using the inbuilt default semantic model.

|Table 1|Table 2|Cardinality|Cross-filter direction|Make this relationship acitve|
|--|--|--|--|--|
|stockmarketdata.DateID|calendar.DateID|Many to one|Single|Yes|
|stockmarketdata.Ticker|companymaster.Symbol|Many to one|Single|Yes|


### Click on each of the columns in the modeling view and go to properties -> advanced and then set the "Sort by Column"

|Table |Column |Sort by Column|Summarize by| Is Hidden |
|--|--|--|--|--|
|calendar|YearMonth|YearMonthID|
|calendar|Month|MonthID|
|calendar|Week|WeekID|
|calendar|Quarter|QuarterID|
|calendar|Year|| None|
|calendar|MonthID|| None|Yes|
|calendar|QuarterID|| None|Yes|
|calendar|WeekID|| None|Yes|



## Step 5. Create reports



## Step 6. Power BI Copilot 

Please note that Copilot is not supported on trial capacities, a minimum of F64 or P1 is required for this.

|Copilot| Prompt|Status|
|--|--|--|
|Power BI|Create a page to examine the daily stock market performance based on opening, closing, high, and low prices|OK|
|Power BI|Create a page to analyze the distribution of IPO years across different sectors and countries|
|Power BI|Create a page to identify trends in trading volume and open interest over time|Ignore|
|Power BI|Create a page to analyze the distribution of industries within each sector|
|Power BI|Create a page to analyze the stock market activity on a monthly basis|
|Power BI|Create a page to identify the best performing sectors based on stock market data|
|Power BI|what are the top 3 symbols with highest volume in year 2022|
|Power BI|Create a page to identify trends and patterns in specific sectors|OK|
|Power BI|(Enable QnA) Question: show top 10 performing stock from last 12 months|
|Power BI|Question: Show top 5 Sectors by total Vol |OK|
|Power BI|Question: Show top 10 Industries by total Vol from last 12 months |OK|
|AI Skills experience| show me the count of stock |
|AI Skills experience| show me unique count of Symbol by industry |



## Step 6. DAX Calculations

|Calc| DAX|
|--|--|
|YTDVol|YTDVol = CALCULATE(sum(stockmarketdata[Vol]),DATESYTD('calendar'[SQLDate]))|
|Prevyear|Prevyear = CALCULATE(sum(stockmarketdata[Vol]),SAMEPERIODLASTYEAR('calendar'[SQLDate]))|
| Calc Column|Total Volume = CALCULATE(SUM(stockmarketdata[Vol]))|
|KeyStock|KeyStock = IF(companymaster[Total Volume] > 10000,"Yes","No")
|Vol|Vol2010 = CALCULATE(sum(stockmarketdata[Vol]),'calendar'[Year] = 2010)
|Vol|Vol2015 = CALCULATE(sum(stockmarketdata[Vol]),'calendar'[Year] = 2015)
|Growth|Growth = DIVIDE([Vol2015],[Vol2010])


