import dlt
import pyspark.sql.functions as F

# Define the schema for reviews
review_schema = """
    review_id STRING,
    user_id LONG,
    product_id STRING,
    rating INT,
    review_text STRING,
    timestamp STRING
"""

@dlt.table(name="reviews_raw")
def read_reviews():
    return (
        spark.read.format("json")
        .schema(review_schema)
        .load("/Volumes/training/default/reviews/")
    )

@dlt.table(name="reviews_cleaned")
def clean_reviews():
    df = dlt.read("reviews_raw")
    # Filter out low ratings and missing fields
    df = df.filter(
        (df.rating >= 3) &
        df.review_id.isNotNull() &
        df.user_id.isNotNull() &
        df.product_id.isNotNull() &
        df.rating.isNotNull() &
        df.review_text.isNotNull() &
        df.timestamp.isNotNull()
    )
    return df

# Add DLT expectations for data quality
@dlt.expect("valid_rating", "rating >= 3")
@dlt.expect("not_null_review_id", "review_id IS NOT NULL")
@dlt.expect("not_null_user_id", "user_id IS NOT NULL")
@dlt.expect("not_null_product_id", "product_id IS NOT NULL")
@dlt.expect("not_null_review_text", "review_text IS NOT NULL")
@dlt.expect("not_null_timestamp", "timestamp IS NOT NULL")
@dlt.table(name="reviews_validated")
def validated_reviews():
    return dlt.read("reviews_cleaned")

# Write to Delta table in training catalog
@dlt.table(name="training.reviews_final")
def write_final_reviews():
    return dlt.read("reviews_validated")
