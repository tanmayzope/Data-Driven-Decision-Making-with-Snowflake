# E-commerce Insights Platform using Snowflake

## Introduction
Welcome to the E-commerce Insights Platform! In this project, we're leveraging Snowflake, Python, and Streamlit to analyze Amazon's e-commerce data. Our goal is simple: decode consumer behavior and marketplace trends. We chose datasets, built a thematic story, and use advanced data modeling. The result? Actionable insights for businesses navigating the dynamic world of online retail. Let's dig into the data and make it work for us.

## Overview

#### **Thematic Story Development**

   #### Data Selection - Snowflake Marketplace

   **Objective:**
   Harnessing data to reveal consumer behavior patterns and product performance on major e-commerce platforms, with a focus on Amazon's expansive retail ecosystem.

   **Datasets:**
   1. Amazon and E-commerce Websites: Product Views and Purchases [link](https://app.snowflake.com/marketplace/listing/GZT1ZA3NK6/similarweb-ltd-amazon-and-e-commerce-websites-product-views-and-purchases?search=amazon)
   2. Amazon Sales and Market Share [link](https://app.snowflake.com/marketplace/listing/GZSOZ18UTU/jungle-scout-amazon-sales-and-market-share-demo?search=amazon)
   3. Calendar Data with Date Dimensions [link](https://app.snowflake.com/marketplace/listing/GZSUZCCDD/infocepts-calendar-data-with-date-dimensions-free-ready-to-use)

### Resources
* [Codelab](https://codelabs-preview.appspot.com/?file_id=1w7wjX9IvupqjPWn8kvJ18s-AhFWQPPdK70WeTuZzC5E#0)
* Demo - [YouTube Demo](https://youtu.be/ntNEQ7fpLYg)

### Tech Stack
Python | Snowflake | Streamlit | GCP | LangChain

### Repo Structure
```
.
├── LICENSE
├── README.md
├── Steps
│   ├── 01_setup_snowflake.sql
│   ├── 02_load_data.sql
│   ├── 03_month_to_season_udf
│   ├── 04_rate_conversions_udf
│   ├── 05_product_trend_analysis_sp
│   ├── 06_dimension_model_amazon.py
│   ├── 07_DataTransformation.py
│   └── 08_teardown_script.sql
├── Streamlit
│   ├── app.py
│   └── requirements.txt
├── Views
│   └── views.sql
├── config
├── environment.yml
├── myproject_utils
│   ├── __init__.py
│   └── snowpark_utils.py
├── oryx-build-commands.txt
└── requirements.txt
```

