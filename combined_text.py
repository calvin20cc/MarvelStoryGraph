# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, concat, when, lit, udf
from snowflake.snowpark.types import ArrayType, StringType, IntegerType

# Chunking UDF
def text_chunker(text: str, chunk_size: int = 500, overlap: int = 50) -> list:
    if text is None:
        return []
    chunks = []
    for i in range(0, len(text), chunk_size - overlap):
        chunks.append(text[i:i + chunk_size])
    return chunks
    
def main(session: snowpark.Session): 
    
    # Table name
    tableName = 'marvel.public.events'
    
    # Read data from the table
    events_df = session.table(tableName)
    
    # Combine all columns into a single text column
    combined_df = events_df.with_column(
        "COMBINED_TEXT", 
        concat(
            when(col("TITLE").is_not_null(), concat(col("TITLE"), lit(" is an event. "))).otherwise(lit("")),
            when(col("TITLE").is_not_null() & col("DESCRIPTION").is_not_null(), concat(col("TITLE"), lit(" is about "), col("DESCRIPTION"), lit(". "))).otherwise(lit("")),
            when(col("TITLE").is_not_null() & col("START_TIME").is_not_null(), concat(col("TITLE"), lit(" started on "), col("START_TIME"), lit(". "))).otherwise(lit("")),
            when(col("TITLE").is_not_null() & col("END_TIME").is_not_null(), concat(col("TITLE"), lit(" ended on "), col("END_TIME"), lit(". "))).otherwise(lit(""))
        )
    )
    
    # Display the resulting dataframe
    combined_df.select(col("COMBINED_TEXT")).show()
    
    # Save the chunked data
    combined_df.write.mode("overwrite").save_as_table("marvel.public.events_combined")

    # Return the modified dataframe
    return combined_df