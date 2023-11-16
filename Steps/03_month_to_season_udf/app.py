import sys
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType, StringType

# Define the Python function
def main(month_number: int) -> str:
    if month_number in (3, 4, 5):
        return 'Spring'
    elif month_number in (6, 7, 8):
        return 'Summer'
    elif month_number in (9, 10, 11):
        return 'Fall'
    elif month_number in (12, 1, 2):
        return 'Winter'
    else:
        return 'Please enter a valid month number'

# Register the function as a UDF in Snowflake
# get_season_udf = udf(get_season, return_type=StringType(), input_types=[IntegerType()])

if __name__ == '__main__':
    if len(sys.argv) > 1:
        month_arg = int(sys.argv[1])  # Convert the string argument to an integer
        print(main(month_arg))
    else:
        print("No month number provided")
