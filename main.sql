/*
CONSUMER PRICE SURVEYS DATA EXPLORATION
Consumer Price Surveys 2009-2016 retrieved from https://data.gov.jm/dataset/consumer-prices/resource/9eddc57f-c00a-4d3a-bf09-6aa59f1f7afa 

SKILLS USED: Data Cleaning, Joins, CTE's, Temp Tables, Aggregate Functions, Windows, Triggers, Creating Views, Converting Data Types

*/

SELECT * FROM main;
DESCRIBE main;

------------------------------------------------------
/* CREATING THE TABLES*/
------------------------------------------------------

--Create MAIN table that will store all the data
CREATE TABLE main (
    Code INT PRIMARY KEY,
    Date DATE,
    Type VARCHAR (20),
    Location VARCHAR(50),
    Town VARCHAR(50),
    Parish VARCHAR(20),
    Shop_Type varchar(20),
    Item varchar(50),
    Goods VARCHAR(50),
    Unit varchar(20),
    Price decimal(8,2)
);

/* --Test by inserting a value into the table
INSERT INTO main VALUES (6053,'2009-11-12','Grocery','SHOPPER'' S FAIR (PORTMORE MALL)','PORTMORE','St. Catherine','Supermarket','EGGS [TX]','LOCAL','1 doz',172.90);
SELECT * FROM main;
DELETE FROM main WHERE Code = 6053;
-- Test complete. Table properly formatted */

/* Load csv file into mysql database using the COMMAND LINE */
SET GLOBAL local_infile=1;
LOAD DATA LOCAL INFILE '.../cacSurveys3May2016 CLEANED.csv'
INTO TABLE main 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


------------------------------------------------------
/* DATA CLEANING */
------------------------------------------------------

UPDATE main SET Location ="SHOPPER'S FAIR (PORTMORE MALL)" WHERE LOCATION ='SHOPPER'' S FAIR (PORTMORE MALL)';
UPDATE main SET Price = NULL WHERE Price = 0;

SELECT DISTINCT YEAR(Date) FROM main; -- Check if any discrepancies are in the years

-- UPDATING FAULTY DATES AND CODES
UPDATE main set Date = '2009-12-03' WHERE Date = '2099-12-03'; -- Updating years with errors
UPDATE main set Date = '2010-04-27' WHERE Date = '0210-04-27';
UPDATE main set Date = '2011-06-09' WHERE Date = '0011-06-09';
UPDATE main set Date = '2010-09-28' WHERE Date = '0201-09-28';
UPDATE main set Date = '2011-07-19' WHERE Date = '0201-07-19';
UPDATE main set Date = '2011-08-12' WHERE Date = '0201-08-12';
UPDATE main set Date = '2011-09-08' WHERE Date = '0201-09-08';
UPDATE main set Date = '2012-07-05' WHERE Date = '2101-07-05';
UPDATE main set Date = '2010-09-09' WHERE code BETWEEN 233428 AND 233543; -- these dates were 0000, but should be 2010-09-09
UPDATE main set Date = '2011-01-26' WHERE code BETWEEN 319672 AND 319674;
UPDATE main set Date = '2011-04-27' WHERE code BETWEEN 369092 AND 369206;
UPDATE main set Date = '2011-07-08' WHERE code BETWEEN 429168 AND 429284;
UPDATE main set Date = '2011-12-09' WHERE code BETWEEN 544465 AND 544579;
UPDATE main set Date = '2011-12-08' WHERE code between 550316 AND 550430;
UPDATE main set Date = '2012-01-19' WHERE code between 566716 AND 566774;
UPDATE main set Date = '2009-11-26' WHERE Date = '2006-11-26'; -- all data values are between 2009 and 2016. Surrounding values are 2009
UPDATE main set Date = '2011-08-12' WHERE Date = '2001-08-12' OR Date = '2020-08-12';


UPDATE main JOIN main_copy on main.code = main_copy.code SET main.Date = main_copy.Date
 --Update records from one table to another. I accidentally deleted all records in main table, had to copy the set and fix using a join

------------------------------------------------------
/* QUERIES */
------------------------------------------------------

--How many supermarkets are in Portmore?
SELECT 
    COUNT(DISTINCT Location) AS Portmore_Supermarkets -- Since the data is collected over time, I queried 'DISTINCT Location' to count the number of unique locations
FROM 
    main 
WHERE Shop_Type = 'Supermarket' AND Town = 'Portmore'; --fulfills both conditions
-- Conclusion: Portmore has 8 supermarkets 



--Which parish has the most supermarkets?
SELECT
        Parish,
        COUNT(DISTINCT LOCATION) as no_of_supermarkets
    FROM
        main
    WHERE 
        Shop_Type = 'Supermarket'
    GROUP BY Parish                        -- This groups all locations by parish
    ORDER BY no_of_supermarkets DESC       -- The highest value will come first
LIMIT 1;                                   -- This restricts query to the first value, which automatically will be the highest
-- Conclusion: St. Andrew has the most supermarkets - 28


--Which Location sold the cheapest LOCAL eggs on average over the period?
-- (1) Create CTE (2) Find average price of LOCAL eggs during the period (3) select lowest average price from CTE
WITH cte_eggs as (
SELECT
    DISTINCT Location, Town, CONCAT('$',ROUND(AVG(Price),2)) AS avg_price
FROM 
    main 
WHERE 
    Item LIKE 'EGGS%' and Goods = 'LOCAL' GROUP BY Location, Town
    )
SELECT * FROM cte_eggs WHERE 
    avg_price = (
    SELECT Min(avg_price) from cte_eggs
);
-- Conclusion: MANDEVILLE CASH & CARRY @ an average price of $180.68


-- How have Whole Chicken prices changed by years at Shopper's Fair locations in Montego Bay?
with c as (SELECT 
    DISTINCT Location, 
    year(date) year, 
    ROUND(AVG(Price), 2) AS yearly_price
FROM 
    main 
WHERE 
    item = 'WHOLE CHICKEN (Grade A, Frozen) [NT]' AND 
    Location = "SHOPPER'S FAIR" AND
    Town = 'Montego Bay'
GROUP BY year)
SELECT *, concat(ROUND((yearly_price/(LAG(yearly_price) OVER (PARTITION BY Location order by year)))*100, 1)-100,'%') AS percentage_increase FROM c;

--- ALTERNATELY
SELECT 
    DISTINCT Location, 
    year(date) year, 
    ROUND(AVG(Price), 2) AS yearly_price,
    ROUND((AVG(Price)/(LAG(AVG(Price)) OVER (PARTITION BY Location order by year(date))))*100, 1)-100 AS percentage_increase
FROM 
    main 
WHERE 
    item = 'WHOLE CHICKEN (Grade A, Frozen) [NT]' AND 
    Location = "SHOPPER'S FAIR" AND
    Town = 'Montego Bay'
GROUP BY year;
-- CONCLUSION: Prices increased year on year, the greatest being from 2012-13 and least being 2014-15



-- What items do the supermarkets in Montego Bay sell?
select distinct location from main where town="montego bay" and type = 'grocery'; -- First I found all the supermarkets in Montego Bay.
SELECT 
    Item, -- Since the data is spread over the years, I'll use item as the group by clause
    MIN(CASE WHEN Location="HI-LO BASIX" THEN '✅' ELSE '❌' END) AS "HI-LO BASIX",             -- CASE statements create the PIVOT Table for each Location. MIN returns the entry with the lowest row number
    MIN(CASE WHEN Location="A & H SUPERMARKET" THEN '✅' ELSE '❌' END) AS "A & H SUPERMARKET",
    MIN(CASE WHEN Location="SUPER PLUS" THEN '✅' ELSE '❌' END) AS "SUPER PLUS",
    MIN(CASE WHEN Location="MEGA MART" THEN '✅' ELSE '❌' END) AS "MEGA MART",
    MIN(CASE WHEN Location="SHOPPER'S FAIR" THEN '✅' ELSE '❌' END) AS "SHOPPER'S FAIR",
    MIN(CASE WHEN Location="BAY CITY SUPERMARKET- MONTEGO BAY" THEN '✅' ELSE '❌' END) AS "BAY CITY SUPERMARKET- MONTEGO BAY",
    MIN(CASE WHEN Location="CONSUMERS' MEAT PLUS" THEN '✅' ELSE '❌' END) AS "CONSUMERS' MEAT PLUS"
FROM 
    (SELECT 
        Location, 
        Item, 
        ROW_NUMBER() over (Partition by Location) AS rn 
    FROM 
        main 
    WHERE 
        Town = 'Montego Bay' and Type = 'Grocery'
) as t -- Create a window subquery with row numbers for the MIN function
GROUP BY 
    Item -- Group the items and choose the value for the MINIMUM only from the Group
ORDER BY Item;


-- Store records for gas stations in the Western parishes - St. James, Hanover, and Westmoreland- in a temporary table
CREATE TEMPORARY TABLE temp_wp (
    Code INT PRIMARY KEY,
    Date DATE,
    Location VARCHAR(50),
    Town VARCHAR(50),
    Parish VARCHAR(20),
    Item varchar(50),
    Goods VARCHAR(50),
    Unit varchar(20),
    Price decimal(8,2)
);

INSERT INTO temp_wp
select
    Code,
    Date,
    Location,
    Town,
    Parish,
    Item,
    Goods,
    Unit,
    Price
from main where parish in ('St. James','Westmoreland','Hanover') and type = 'petrol';

--Using the temporary table, list the top 5 most expensive gas stations in the Western Region.
select Location, ROUND(AVG(price), 2) as Price_per_Litre from temp_wp GROUP BY Location order by Price_per_Litre desc limit 5;

-- CONCLUSION: The top 5 most expensive gas stations are RUBiS PETROL (1st, 2nd, 5th) and TEXACO (3rd and 4th)


-- On average, which brand of baked beans is the BEST value for Money?
WITH temp_bb as 
    (
        SELECT 
            Goods, 
            Unit, 
            avg(price) as price 
        FROM 
            main 
        WHERE 
            item = 'BAKED BEANS [TX]' 
        GROUP BY 
            Goods, Unit
        )
SELECT 
    Goods, 
    CONCAT (ROUND((cast(unit as float) / price), 2),' g per $') as unitcost 
FROM temp_bb 
ORDER BY 
    unitcost desc 
limit 1;
-- CONCLUSION: LASCO Baked Beans is the BEST value for money at 3.66 g/$

-- How many times per month did the team take inventory over the period?
SELECT day(Date) day, count(DISTINCT date) from main group by day order by count(distinct date) desc;
-- CONCLUSION: the team typically took inventory on the 10th of the month

SELECT month(Date) month, count(DISTINCT date) from main group by month order by count(distinct date) desc;
-- CONCLUSION: The team typically took inventory during January

SELECT year(Date) year, count(DISTINCT date) from main group by year order by count(distinct date) desc;
-- CONCLUSION: The team took the most inventories in 2012

-- Trigger for new values added to the main table
CREATE TABLE trigger_log (
    log_message VARCHAR(100)
);


CREATE TRIGGER trigger_insert
BEFORE INSERT 
ON main
FOR EACH ROW BEGIN 
    INSERT INTO trigger_log VALUES(NEW.Item);


------------------------------------------------------
/* END */
------------------------------------------------------
