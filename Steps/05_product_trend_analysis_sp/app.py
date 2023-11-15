from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import StructField, StructType, StringType, FloatType, DateType

def get_product_sales(session: Session):
    product_sales = session.table("AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES")
    calendar_data = session.table("CALENDAR_DATA_WITH_DATE_DIMENSIONS__FREE_READY_TO_USE.PUBLIC.CALENDAR_DATA")

    # Perform the join and aggregation as in the SQL stored procedure
    result = product_sales.join(calendar_data, 
                                (product_sales["MONTH"] == calendar_data["MONTH"]) &
                                (product_sales["YEAR"] == calendar_data["YEAR"])
                               ) \
                          .groupBy(product_sales["PRODUCT"], 
                                   product_sales["MAIN_CATEGORY"], 
                                   calendar_data["MONTH"], 
                                   calendar_data["MONTHNAME"]
                                  ) \
                          .agg(F.avg(product_sales["ESTIMATED_PURCHASES"]).alias("AVERAGE_SALES")) \
                          .select(product_sales["PRODUCT"].alias("PRODUCT_ID"), 
                                  product_sales["MAIN_CATEGORY"], 
                                  calendar_data["MONTHNAME"].alias("MONTH_NAME"), 
                                  F.col("AVERAGE_SALES")
                                 )
    return result

def analyze_trends(session: Session, product_sales):
    # Here, we define the logic to determine seasonal trends
    # For simplicity, we'll assume the logic is just to compare with a fixed threshold
    threshold = 100.0  # Example threshold for high/low sales classification
    product_sales = product_sales.withColumn("SEASONAL_TREND", 
                                             F.when(F.col("AVERAGE_SALES") > threshold, F.lit("High")).otherwise(F.lit("Low"))
                                            )
    return product_sales

def product_seasonality_and_trend_analysis(session: Session):
    product_sales = get_product_sales(session)
    trend_analysis = analyze_trends(session, product_sales)
    return trend_analysis



# main function to encapsulate the process logic
def main(session: Session) -> None:
    trend_analysis_results = product_seasonality_and_trend_analysis(session)
    # Assuming you want to display the results or perform further operations
    trend_analysis_results.show()

if __name__ == '__main__':
    import sys
    from pathlib import Path

    # Define the absolute path to the 'utils' directory
    utils_dir = Path(__file__).resolve().parents[1] / 'utils'  # Adjust the number of parents according to the actual location
    sys.path.append(str(utils_dir))

    # Now, try importing the snowpark_utils module
    try:
        from utils import snowpark_utils
    except ImportError as e:
        raise ImportError(f"The snowpark_utils module could not be imported from {utils_dir}. Make sure it is available in the parent directory.") from e

    # Get the Snowpark session using the utility function provided
    session = snowpark_utils.get_snowpark_session()

    # Execute the main function with any additional command line arguments
    try:
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # Pass additional arguments if provided
        else:
            print(main(session))  # Call main without additional arguments
    finally:
        session.close()  # Ensure the session is closed regardless of any errors

