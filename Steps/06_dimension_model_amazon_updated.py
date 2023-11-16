# Function to create dimension tables
def create_schema(session, schema_name):
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
    return f"Schema '{schema_name}' has been created."

def create_and_populate_dimension_tables(session, schema_name):
    # Create and populate the Brand dimension table
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.brand_dimension (
            Brand_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Brand_Name VARCHAR(16777215) UNIQUE
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.brand_dimension (Brand_Name)
        SELECT DISTINCT LEFT(BRAND, 16777215)
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE BRAND IS NOT NULL
    """).collect()

    # Create and populate the Country dimension table
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.country_dimension (
            Country_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Country_Number INTEGER UNIQUE
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.country_dimension (Country_Number)
        SELECT DISTINCT COUNTRY
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE COUNTRY IS NOT NULL
    """).collect()

    # Create and populate the Date dimension table
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.date_dimension (
            Date_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Month_Number INTEGER,
            Year_Number INTEGER,
            UNIQUE(Month_Number, Year_Number)
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.date_dimension (Month_Number, Year_Number)
        SELECT DISTINCT MONTH, YEAR
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE MONTH IS NOT NULL AND YEAR IS NOT NULL
    """).collect()

    # Create and populate the Product dimension table
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.product_dimension (
            Product_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Product_Name VARCHAR(16777215),
            Main_Category VARCHAR(16777215),
            Sub_Category VARCHAR(16777215),
            Title VARCHAR(16777215),
            UNIQUE(Product_Name, Main_Category, Sub_Category, Title)
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.product_dimension (Product_Name, Main_Category, Sub_Category, Title)
        SELECT
        LEFT(PRODUCT, 16777215),             -- Truncate Product_Name to 255 characters
        LEFT(MAIN_CATEGORY, 16777215),       -- Truncate Main_Category to 255 characters
        LEFT(SUB_CATEGORY, 16777215),        -- Truncate Sub_Category to 255 characters
        LEFT(TITLE, 16777215)                -- Truncate Title to 255 characters
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE PRODUCT IS NOT NULL
    """).collect()

    # Create and populate the Site dimension table
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.site_dimension (
            Site_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Site_Name VARCHAR(16777215) UNIQUE
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.site_dimension (Site_Name)
        SELECT DISTINCT LEFT (SITE, 16777215)
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE SITE IS NOT NULL
    """).collect()

    return "Dimension tables have been created and populated."


# Function to create the fact table
def create_fact_table(session, schema_name):
    # Fact table
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.fact_sales (
            Fact_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Date_ID INTEGER,
            Brand_ID INTEGER,
            Country_ID INTEGER,
            Product_ID INTEGER,
            Site_ID INTEGER,
            Estimated_Purchases FLOAT,
            Estimated_Views FLOAT,
            FOREIGN KEY (Date_ID) REFERENCES date_dimension(Date_ID),
            FOREIGN KEY (Brand_ID) REFERENCES brand_dimension(Brand_ID),
            FOREIGN KEY (Country_ID) REFERENCES country_dimension(Country_ID),
            FOREIGN KEY (Product_ID) REFERENCES product_dimension(Product_ID),
            FOREIGN KEY (Site_ID) REFERENCES site_dimension(Site_ID)
        )
    """).collect()

    # Populate the Fact table with data from the original table and keys from dimension tables
    session.sql(f"""
        INSERT INTO {schema_name}.fact_sales (Date_ID, Brand_ID, Country_ID, Product_ID, Site_ID, Estimated_Purchases, Estimated_Views)
        SELECT
            dd.Date_ID,
            bd.Brand_ID,
            cd.Country_ID,
            pd.Product_ID,
            sd.Site_ID,
            o.ESTIMATED_PURCHASES,
            o.ESTIMATED_VIEWS
        FROM
            AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES o
        JOIN date_dimension dd ON o.MONTH = dd.Month_Number AND o.YEAR = dd.Year_Number
        JOIN brand_dimension bd ON o.BRAND = bd.Brand_Name
        JOIN country_dimension cd ON o.COUNTRY = cd.Country_Number
        JOIN product_dimension pd ON o.PRODUCT = pd.Product_Name
        JOIN site_dimension sd ON o.SITE = sd.Site_Name
    """).collect()



    # Additional code to integrate calendar data from dataset 2 into the date_dimension
    # Assuming the date_dimension table already has Date_ID, Month_Number, and Year_Number
    session.sql(f"""
        ALTER TABLE {schema_name}.date_dimension
        ADD IF NOT EXISTS Day_Name VARCHAR(16777215),
        ADD IF NOT EXISTS Week_Number INTEGER,
        ADD IF NOT EXISTS Quarter_Number INTEGER
    """).collect()

    # This is a placeholder for the actual insert statement, you would need to tailor this to your data
    session.sql(f"""
        INSERT INTO {schema_name}.date_dimension (Month_Number, Year_Number, Day_Name, Week_Number, Quarter_Number)
        SELECT DISTINCT MONTH(DATE), YEAR(DATE), DAYNAME, WEEKOFYEAR, QUARTER
        FROM CALENDAR_DATA_WITH_DATE_DIMENSIONS__FREE_READY_TO_USE.PUBLIC.CALENDAR_DATA
        WHERE DATE IS NOT NULL
        ON DUPLICATE KEY UPDATE
            Day_Name = VALUES(Day_Name),
            Week_Number = VALUES(Week_Number),
            Quarter_Number = VALUES(Quarter_Number)
    """).collect()

    # Additional code to integrate sales data from dataset 3 into the fact_sales
    # This assumes that dataset 3 contains a reference to the date and product that can be joined with the existing dimension tables
    session.sql(f"""
        ALTER TABLE {schema_name}.fact_sales
        ADD IF NOT EXISTS Weekly_Estimated_Revenue FLOAT,
        ADD IF NOT EXISTS Weekly_Estimated_Sales INTEGER
    """).collect()

    # Placeholder for the actual insert statement for integrating dataset 3 into fact_sales
    # ...

def main(session, *args):
    schema_name = "PRODUCT_VIEWS_AND_PURCHASES_DIM_MODEL"
    print(create_schema(session, schema_name))   
    
    create_and_populate_dimension_tables(session, schema_name)
    create_fact_table(session, schema_name)

    return "Dimension and Fact tables have been created and populated."


# Entry point for the script
if __name__ == '__main__':
    import os, sys

    # Gets the directory where the current file is located
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate up two levels to the root of your project
    root_dir = os.path.dirname(current_dir)

    # Adds the root directory to the sys.path to find myproject_utils
    sys.path.append(root_dir)

    # Imports snowpark_utils from your project utilities
    from myproject_utils import snowpark_utils
    # Gets the Snowpark session
    session = snowpark_utils.get_snowpark_session()

    # Try block to ensure that the session is closed properly
    try:
        # Execute the main function with any additional command line arguments
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # Pass additional arguments if provided
        else:
            print(main(session))  # Call main without additional arguments
    finally:
        # Close the session
        session.close()
