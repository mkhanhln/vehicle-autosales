# Vehicle Auto Sales — Automotive Market Analytics

> End-to-end data analytics project on ~550,000 vehicle auction records, covering data engineering, SQL business intelligence, Python EDA, and Power BI dashboarding.

---

## Overview

This project builds a full analytics pipeline on a real-world automotive auction dataset. Raw, messy data is ingested, cleaned, and transformed into a structured format ready for multi-dimensional analysis. Business insights are extracted via SQL modules and Python visualizations, then presented through an interactive Power BI Intelligence Dashboard.

---

## Dataset

| Attribute | Details |
|---|---|
| **File** | `car_prices.csv` |
| **Size** | ~550,000 records |
| **Key Fields** | `year` · `make` · `model` · `trim` · `body` · `transmission` · `vin` · `state` · `condition` · `odometer` · `color` · `interior` · `seller` · `mmr` · `sellingprice` · `saledate` |

> ⚠️ The dataset is **not included** in this repository (personal/private data).
> To run the project locally, place your own `car_prices.csv` file inside the `data/` folder and ensure it contains the columns listed above.

---

## Tools & Technologies

| Category | Tools |
|---|---|
| **Language** | Python 3.13.3 |
| **Libraries** | Pandas, NumPy, Matplotlib, Seaborn, Scikit-learn |
| **Database** | SQL Server (T-SQL - `BULK INSERT`, `SELECT INTO`, Window Functions, CTEs) |
| **BI Dashboard** | Power BI (DAX, slicers, KPI cards, donut/bar/line charts) |
| **Environment** | Jupyter Notebook |

---

## Project Workflow

### Phase 0 - Raw Data Ingestion
- Created `dbo.CarPrices` table in SQL Server
- Bulk-loaded `car_prices.csv` (~550K rows) using `BULK INSERT`
- Mirrored in Python with `pd.read_csv()` using `low_memory=False`

### Phase 1 - Data Engineering Pipeline
Transformed raw data into `dbo.Cleaned_CarPrices` (SQL) / `df_final` (Python):

- **Make Standardization** — resolved typos and aliases (`'ford tk'` → `'Ford'`, `'mercedes-b'` → `'Mercedes-Benz'`, `'vw'` → `'Volkswagen'`)
- **Body Type Grouping** — normalized 30+ raw values into 8 clean categories (SUV, Sedan, Coupe, Pickup Truck, Van, Hatchback, Wagon, Convertible)
- **Transmission Mapping** — classified as `Automatic`, `Manual`, or `Unknown`
- **Condition Grading** — converted raw 0–50 scores into a 6-tier label (`0 - Unknown` through `5 - Excellent`)
- **Calculated Columns** — `Profit/Loss = Selling Price − MMR`, `Vehicle Age = 2015 − Year`
- **Date Parsing** — extracted sale date from raw string using substring + `TRY_CAST` / `pd.to_datetime`
- **Filters Applied** — valid 2-char state codes, model years 1990–2016, selling price > $100, non-null VINs (≥17 chars)

### Phase 2 - Exploratory Data Analysis (Python)

**Executive KPI Summary:**
- Total Revenue, Average Sale Price, Average Mileage, Average Profit/Loss vs. MMR

**Visualizations:**
1. **Market Inventory Concentration** — Top 10 brands by units sold (horizontal bar chart)
2. **Price Distribution & Market Segmentation** — Sale price histogram with median/average reference lines and budget segment highlight
3. **Asset Depreciation Curve** — Average sale price by 10k-mile mileage brackets (line + area chart), with 100k-mile psychological barrier annotated
4. **Regional Price Accuracy** — Profit/Loss variance by state, top 10 states (notched boxplot)
5. **Sales Velocity** — Daily transaction volume + 7-day rolling average trend line with market peak annotation

### Phase 3 - SQL Business Intelligence (5 Modules, 20 Queries)

| Module | Focus |
|---|---|
| **Module 1** | Market Composition & Inventory Liquidity |
| **Module 2** | Pricing Architecture & Profitability |
| **Module 3** | Asset Depreciation & Condition ROI |
| **Module 4** | Geographical & Seller Performance |
| **Module 5** | Advanced BI & Executive Reporting |

**Highlights:**
- Window functions (`SUM() OVER()`, `PARTITION BY`) for market share calculations
- `STDEV` for brand price volatility / risk scoring
- Arbitrage detection — cars sold >30% below MMR
- `LAG()` for year-over-year depreciation
- `DENSE_RANK()` + CTE for best-selling model per brand
- `PERCENT_RANK()` for luxury tier outlier detection
- Rolling 7-day average for sales momentum
- Final executive `VIEW`: `dbo.v_Executive_Brand_Health`

---

## Power BI Dashboard

**Auto Sales Intelligence Dashboard** featuring:

| Visual | Description |
|---|---|
| **KPI Cards** | Sum of Profit/Loss (`-$87.44M`) · Count of Makes (`548K`) |
| **Sale Price by Model** | Horizontal bar — F-150, Altima, Escape, Fusion among top sellers |
| **Condition Grade** | Donut chart — 5-tier breakdown (Excellent → Unknown) |
| **Hot Month** | Bar chart of units sold by month (February peak) |
| **Drill-Down Table** | Make · Model · Trim · Body Type · Colors · Transmission · Condition · Seller · MMR · Sale Price · Profit/Loss · Mileage |
| **Filters** | Make slicer · Date of Sale range slider |

> 📎 *[Link to Power BI Dashboard](#)* — (https://github.com/mkhanhln/vehicle-autosales/blob/main/Project.pbix)

---

## How to Run

### Prerequisites
```bash
pip install pandas numpy matplotlib seaborn scikit-learn jupyter
```

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/mkhanhln/vehicle-autosales.git
cd vehicle-autosales

# 2. Install dependencies
pip install -r requirements.txt

# 3. Add your dataset
# Place car_prices.csv inside the data/ folder:
# vehicle-autosales/data/car_prices.csv

# 4. Update the file path in the notebook (if needed)
# file_path = 'data/car_prices.csv'

# 5. Launch Jupyter and run all cells
jupyter notebook notebooks/Vehicle_Sales_-_Python.ipynb
```

### SQL (SQL Server)
```sql
-- 1. Run Phase 0 to create dbo.CarPrices
-- 2. Update the BULK INSERT path to your local car_prices.csv
-- 3. Run Phase 1 to build dbo.Cleaned_CarPrices
-- 4. Run Modules 1–5 for business analytics queries
```

### Power BI
Open `dashboard/vehicle_sales_dashboard.pbix` in Power BI Desktop and connect it to your local `dbo.Cleaned_CarPrices` table or cleaned CSV export.

---

## Project Structure

```
📦 vehicle-autosales
 ┣ 📂 data
 ┃ ┗ 📄 car_prices.csv        ← not included, add your own
 ┣ 📂 notebooks
 ┃ ┗ 📓 Vehicle_Sales_-_Python.ipynb
 ┣ 📂 sql
 ┃ ┗ 📄 AutoSales.sql
 ┣ 📂 dashboard
 ┃ ┗ 📄 vehicle_sales_dashboard.pbix
 ┣ 📄 requirements.txt
 ┗ 📄 README.md
```

---

## 👤 Author

**mkhanhln**
- GitHub: [@mkhanhln](https://github.com/mkhanhln)
---
## 📃 License

This project is licensed under the [MIT License](LICENSE).
