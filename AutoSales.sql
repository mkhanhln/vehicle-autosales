/*******************************************************************************
PROJECT: Automotive Market Analytics & Data Engineering
DATASET: car_prices.csv (~550,000 records)
PURPOSE: Comprehensive End-to-End SQL Project for Business Intelligence.
*******************************************************************************/

-- =============================================
-- PHASE 0: RAW DATA INGESTION
-- =============================================
IF OBJECT_ID('dbo.CarPrices','U') IS NOT NULL DROP TABLE dbo.CarPrices;
GO
CREATE TABLE dbo.CarPrices (
    [year] INT, [make] NVARCHAR(100), [model] NVARCHAR(100), [trim] NVARCHAR(100),
    [body] NVARCHAR(100), [transmission] NVARCHAR(50), [vin] NVARCHAR(50),
    [state] NVARCHAR(50), [condition] FLOAT, [odometer] FLOAT, [color] NVARCHAR(50),
    [interior] NVARCHAR(50), [seller] NVARCHAR(255), [mmr] FLOAT,
    [sellingprice] FLOAT, [saledate] NVARCHAR(100) 
);
GO
BULK INSERT dbo.CarPrices FROM 'data/car_prices.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
GO
-- =============================================
-- PHASE 1: THE DATA ENGINEERING PIPELINE
IF OBJECT_ID('dbo.Cleaned_CarPrices', 'U') IS NOT NULL 
    DROP TABLE dbo.Cleaned_CarPrices;
GO


-- =============================================
/* CONCEPT: Data Cleaning & Materialization.
GOAL: Transform raw "dirty" data into a standardized table for high-speed analysis. */
-- 2. Create the materialization script (SELECT INTO dbo.Cleaned_CarPrices):

-- a. Select DISTINCT records.
-- b. Standardize [Make] names (handling typos like 'ford tk' or 'mercedes-b').
-- c. Standardize [Body Type] into major categories (SUV, Sedan, Coupe, etc.).
-- d. Standardize [Transmission] to 'Automatic', 'Manual', or 'Unknown'.
-- e. Logic to convert [condition] scores into readable [Condition Grade] (1-5).
-- f. Handle NULL values for Color, Interior, and Seller.
-- g. Calculated Columns: [Profit/Loss] (Selling Price - MMR) and [Vehicle Age].
-- h. Filter: Only keep valid States (2 chars), years 1990-2016, and valid VINs.
-- 1. Check if dbo.Cleaned_CarPrices exists and drop if necessary.
SELECT 
    DISTINCT
    [year] AS [Model Year],
    -- 1. Standardized Make Logic
    CASE 
        WHEN LOWER(make) IN ('dodge tk', 'dodge') THEN 'Dodge'
        WHEN LOWER(make) IN ('ford tk', 'ford truck', 'ford') THEN 'Ford'
        WHEN LOWER(make) IN ('chev truck', 'chevrolet') THEN 'Chevrolet'
        WHEN LOWER(make) IN ('gmc truck', 'gmc') THEN 'GMC'
        WHEN LOWER(make) IN ('mazda tk', 'mazda') THEN 'Mazda'
        WHEN LOWER(make) IN ('hyundai tk', 'hyundai') THEN 'Hyundai'
        WHEN LOWER(make) IN ('landrover', 'land rover') THEN 'Land Rover'
        WHEN LOWER(make) IN ('vw', 'volkswagen') THEN 'Volkswagen'
        WHEN LOWER(make) IN ('mercedes', 'mercedes-b', 'mercedes-benz') THEN 'Mercedes-Benz'
        WHEN LOWER(make) IN ('acura', 'cadillac', 'honda', 'nissan', 'fiat', 'kia', 'mini', 'smart', 'infiniti', 'lexus', 'porsche', 'audi', 'bmw', 'buick', 'chrysler', 'jaguar', 'jeep', 'lincoln', 'mitsubishi', 'subaru', 'toyota', 'volvo', 'ferrari', 'lamborghini', 'bentley', 'rolls-royce', 'aston martin', 'maserati', 'fisker', 'lotus', 'tesla', 'pontiac', 'saturn', 'mercury', 'hummer', 'oldsmobile', 'isuzu', 'geo', 'plymouth', 'saab', 'daewoo', 'suzuki', 'airstream', 'scion', 'ram') 
            THEN UPPER(LEFT(make,1)) + LOWER(SUBSTRING(make,2,LEN(make)))
        WHEN LOWER(make) = 'dot' THEN NULL 
        ELSE make 
    END AS [Make],
    
    UPPER(model) AS [Model], 
    [trim], 

    -- 2. Standardized Body Types
    CASE 
        WHEN UPPER(body) IN ('SUV') THEN 'SUV'
        WHEN UPPER(body) IN ('SEDAN', 'G SEDAN', 'ELANTRA COUPE', 'TSX SPORT WAGON') THEN 'Sedan'
        WHEN UPPER(body) IN ('COUPE', 'G COUPE', 'GENESIS COUPE', 'CTS COUPE', 'CTS-V COUPE', 'Q60 COUPE', 'G37 COUPE', 'KOUP') THEN 'Coupe'
        WHEN UPPER(body) IN ('CONVERTIBLE', 'G CONVERTIBLE', 'BEETLE CONVERTIBLE', 'Q60 CONVERTIBLE', 'G37 CONVERTIBLE', 'GRANTURISMO CONVERTIBLE') THEN 'Convertible'
        WHEN UPPER(body) IN ('CREW CAB', 'SUPERCREW', 'SUPERCAB', 'REGULAR CAB', 'EXTENDED CAB', 'QUAD CAB', 'DOUBLE CAB', 'CREWMAX CAB', 'KING CAB', 'ACCESS CAB', 'MEGA CAB', 'XTRACAB', 'REGULAR-CAB', 'CAB PLUS 4', 'CAB PLUS') THEN 'Pickup Truck'
        WHEN UPPER(body) IN ('MINIVAN', 'VAN', 'E-SERIES VAN', 'PROMASTER CARGO VAN', 'TRANSIT VAN', 'RAM VAN') THEN 'Van'
        WHEN UPPER(body) IN ('HATCHBACK') THEN 'Hatchback'
        WHEN UPPER(body) IN ('WAGON', 'CTS WAGON', 'CTS-V WAGON') THEN 'Wagon'
        ELSE 'Other'
    END AS [Body Type],

    [vin],
    CASE 
        WHEN transmission LIKE '%auto%' THEN 'Automatic' 
        WHEN transmission LIKE '%man%' THEN 'Manual' 
        ELSE 'Unknown' 
    END AS [Transmission],
    
    UPPER(state) AS [State Code], 
    [condition] AS [Raw Condition],
    
    -- 3. Normalized Condition Grade Logic
    CASE 
        WHEN condition IS NULL OR condition = 0 THEN '0 - Unknown'
        WHEN condition > 40 THEN '5 - Excellent'
        WHEN condition > 30 THEN '4 - Very Good'
        WHEN condition > 20 THEN '3 - Good'
        WHEN condition > 10 THEN '2 - Fair'
        ELSE '1 - Poor'
    END AS [Condition Grade],

    [odometer] AS [Mileage], 
    UPPER(ISNULL(color, 'Unknown')) AS [Exterior Color],
    UPPER(ISNULL(interior, 'Unknown')) AS [Interior Color], 
    UPPER(ISNULL(seller, 'Unknown')) AS [Seller],
    [sellingprice] AS [Sale Price], 
    [mmr] AS [Estimated Value], 
    ([sellingprice] - [mmr]) AS [Profit/Loss],
    
    TRY_CAST(SUBSTRING(saledate, 5, 11) AS DATE) AS [Date of Sale], 
    (2015 - [year]) AS [Vehicle Age]

INTO dbo.Cleaned_CarPrices
FROM dbo.CarPrices
WHERE LEN(state) = 2 
    AND [year] BETWEEN 1990 AND 2016 
    AND sellingprice > 100 
    AND [vin] IS NOT NULL 
    AND [make] IS NOT NULL
    AND [model] IS NOT NULL
    AND [trim] IS NOT NULL
    AND LEN(vin) >= 17;
GO

SELECT * FROM dbo.Cleaned_CarPrices

-- =============================================
-- MODULE 1: MARKET COMPOSITION & INVENTORY LIQUIDITY
-- =============================================

-- Q1: Market Share by Body Type
-- Requirement: Calculate total volume and market share percentage per body type using a Window Function.
SELECT [Body Type], COUNT(*) AS Volume, 
       CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(10,2)) AS Market_Share_Pct
FROM dbo.Cleaned_CarPrices 
GROUP BY [Body Type] 
ORDER BY Volume DESC;

-- Q2: Brand Liquidity (Top 10 High-Volume Makes)
-- Requirement: Identify the 10 brands with the highest sales volume.
SELECT TOP 10 [Make], COUNT(*) AS Units_Sold 
FROM dbo.Cleaned_CarPrices 
GROUP BY [Make] 
ORDER BY Units_Sold DESC;

-- Q3: Inventory Freshness (Model Year Distribution)
-- Requirement: List units sold grouped by Model Year in descending order.
SELECT [Model Year], COUNT(*) AS Units 
FROM dbo.Cleaned_CarPrices 
GROUP BY [Model Year] 
ORDER BY [Model Year] DESC;

-- Q4: Transmission Scarcity Index
-- Requirement: Calculate the percentage of Manual vs Automatic specifically for Coupes and Sedans.
SELECT [Body Type], [Transmission], COUNT(*) AS Count,
       CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY [Body Type]) AS DECIMAL(10,2)) AS Segment_Pct
FROM dbo.Cleaned_CarPrices 
WHERE [Body Type] IN ('Coupe', 'Sedan') 
GROUP BY [Body Type], [Transmission];


-- =============================================
-- MODULE 2: PRICING ARCHITECTURE & PROFITABILITY
-- =============================================

-- Q5: Average Sale Price by Brand (Market Tiering)
-- Requirement: Find the average Sale Price per Make, formatted to 2 decimal places.
SELECT [Make], CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price 
FROM dbo.Cleaned_CarPrices 
GROUP BY [Make] 
ORDER BY Avg_Price DESC;
-- Q6: Price Accuracy Audit (Actual Sale vs. Estimated Value)
-- Requirement: Calculate the average [Profit/Loss] variance per Make to check MMR accuracy.
SELECT [Make], CAST(AVG([Profit/Loss]) AS DECIMAL(10,2)) AS Avg_Price_Variance 
FROM dbo.Cleaned_CarPrices 
GROUP BY [Make] 
ORDER BY Avg_Price_Variance DESC;

-- Q7: Market Volatility Index (Risk Measurement)
-- Requirement: Use STDEV to find brands with the most unpredictable pricing (Sample size > 500).
SELECT [Make], CAST(STDEV([Sale Price]) AS DECIMAL(10,2)) AS Price_Volatility, COUNT(*) AS Vol
FROM dbo.Cleaned_CarPrices 
GROUP BY [Make] 
HAVING COUNT(*) > 500 
ORDER BY Price_Volatility DESC;
-- Q8: The "Steal Finder" (Arbitrage Detection)
-- Requirement: Identify individual cars sold at more than 30% below their Estimated Value (MMR).
SELECT [Make], [Model], [Estimated Value], [Sale Price], CAST([Profit/Loss] AS DECIMAL(10,2)) AS Price_Diff
FROM dbo.Cleaned_CarPrices 
WHERE [Sale Price] < ([Estimated Value] * 0.7) AND [Estimated Value] > 5000;
-- =============================================
-- MODULE 3: ASSET DEPRECIATION & CONDITION ROI
-- =============================================

-- Q9: The "Condition Premium" (Value of a Grade)
-- Requirement: Show Average Price and Average Mileage grouped by the Condition Grade.
SELECT [Condition Grade], CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price,
		CAST(AVG([Mileage]) AS DECIMAL(10,2)) AS Avg_Mileage
FROM dbo.Cleaned_CarPrices 
GROUP BY [Condition Grade] 
ORDER BY [Condition Grade] DESC;

-- Q10: Mileage Decay Bins (Value Loss Per 20k Miles)
-- Requirement: Bin mileage into 20,000-mile increments and show the average price for each bracket.
SELECT FLOOR([Mileage]/20000)*20000 AS Mileage_Bracket, CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price 
FROM dbo.Cleaned_CarPrices 
GROUP BY FLOOR([Mileage]/20000)*20000 
ORDER BY Mileage_Bracket DESC;


-- Q11: Year-over-Year Depreciation (LAG Function)
SELECT [Vehicle Age], CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price,
       CAST(LAG(AVG([Sale Price])) OVER (ORDER BY [Vehicle Age]) - AVG([Sale Price]) AS DECIMAL(10,2)) AS Annual_Value_Drop
FROM dbo.Cleaned_CarPrices GROUP BY [Vehicle Age] ORDER BY [Vehicle Age];

-- Q12: The 100k-Mile Psychological Barrier
SELECT CASE WHEN [Mileage] < 100000 THEN 'Pre-100k' ELSE 'Post-100k' END AS Milestone, 
       CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price
FROM dbo.Cleaned_CarPrices WHERE [Mileage] BETWEEN 90000 AND 110000
GROUP BY CASE WHEN [Mileage] < 100000 THEN 'Pre-100k' ELSE 'Post-100k' END;


-- =============================================
-- MODULE 4: GEOGRAPHICAL & SELLER PERFORMANCE
-- =============================================

-- Q13: Regional Sales Hotspots (State Volume)
SELECT TOP 5 [State Code], COUNT(*) AS Sales_Count FROM dbo.Cleaned_CarPrices GROUP BY [State Code] ORDER BY Sales_Count DESC;

-- Q14: Cross-State Price Arbitrage (Truck Segment)
SELECT [State Code], CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price, COUNT(*) AS Vol
FROM dbo.Cleaned_CarPrices WHERE [Body Type] = 'Pickup Truck' 
GROUP BY [State Code] HAVING COUNT(*) > 100 ORDER BY Avg_Price DESC;

-- Q15: Seller Revenue Concentration (Total Sales per Seller)
SELECT TOP 10 [Seller], CAST(SUM([Sale Price]) AS DECIMAL(15,2)) AS Total_Revenue 
FROM dbo.Cleaned_CarPrices GROUP BY [Seller] ORDER BY Total_Revenue DESC;

-- Q16: State-Level Quality Audit (Source Optimization)
SELECT [State Code], CAST(AVG([Raw Condition]) AS DECIMAL(10,2)) AS Avg_Condition 
FROM dbo.Cleaned_CarPrices GROUP BY [State Code] ORDER BY Avg_Condition DESC;


-- =============================================
-- MODULE 5: ADVANCED BI & EXECUTIVE REPORTING
-- =============================================

-- Q17: Best-Selling Model per Brand (CTE + DENSE_RANK)
WITH ModelRank AS (
    SELECT [Make], [Model], COUNT(*) AS Vol, DENSE_RANK() OVER(PARTITION BY [Make] ORDER BY COUNT(*) DESC) as rnk 
    FROM dbo.Cleaned_CarPrices GROUP BY [Make], [Model])
SELECT [Make], [Model], Vol FROM ModelRank WHERE rnk = 1 ORDER BY Vol DESC;

-- Q18: Luxury Tier Outlier Detection (Top 1% Percentile)
SELECT [Make], [Model], CAST([Sale Price] AS DECIMAL(10,2)) AS Sale_Price 
FROM (SELECT *, PERCENT_RANK() OVER(ORDER BY [Sale Price]) AS p_rank FROM dbo.Cleaned_CarPrices) t 
WHERE p_rank > 0.99 ORDER BY [Sale Price] DESC;

-- Q19: Sales Momentum (7-Day Rolling Average)
SELECT [Date of Sale], COUNT(*) AS Daily_Vol,
       CAST(AVG(COUNT(*)) OVER(ORDER BY [Date of Sale] ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS Rolling_7Day_Avg
FROM dbo.Cleaned_CarPrices GROUP BY [Date of Sale];

-- Q20: The Executive Summary (Final Report View)
CREATE OR ALTER VIEW dbo.v_Executive_Brand_Health AS
SELECT [Make], COUNT(*) AS Total_Volume, 
       CAST(AVG([Sale Price]) AS DECIMAL(10,2)) AS Avg_Price, 
       CAST(AVG([Profit/Loss]) AS DECIMAL(10,2)) AS Avg_Margin
FROM dbo.Cleaned_CarPrices GROUP BY [Make] HAVING COUNT(*) > 500;
GO

SELECT * FROM dbo.v_Executive_Brand_Health ORDER BY Total_Volume DESC;
