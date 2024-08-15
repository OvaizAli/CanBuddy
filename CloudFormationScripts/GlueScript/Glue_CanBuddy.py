import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create a dynamic frame from S3 with specified options
AmazonS3_node1720496648025 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://s3-raw-canbuddy-demo"], "recurse": True},
    transformation_ctx="AmazonS3_node1720496648025"
)

# Drop unnecessary fields from the dynamic frame
DropFields_node1720497770957 = DropFields.apply(
    frame=AmazonS3_node1720496648025,
    paths=["author_fullname", "thumbnail", "domain", "link_flair_richtext", "allow_live_comments", "thumbnail_height", "thumbnail_width", "created_utc", "author", "url", "permalink", "subreddit_subscribers", "is_video", "pwls", "gilded"],
    transformation_ctx="DropFields_node1720497770957"
)

# Apply schema transformation to rename fields and cast types if necessary
mapped_fields = [
    ("subreddit", "string", "subreddit", "string"),
    ("selftext", "string", "selftext", "string"),
    ("title", "string", "title", "string"),
    ("score", "string", "score", "int"),
    ("num_comments", "string", "num_comments", "int"),
    ("upvote_ratio", "string", "upvote_ratio", "decimal"),
    ("link_flair_text", "string", "link_flair_text", "string"),
    ("total_awards_received", "string", "total_awards_received", "int"),
    ("is_original_content", "string", "is_original_content", "string"),
    ("link_flair_type", "string", "link_flair_type", "string"),
    ("is_self", "string", "is_self", "string"),
    ("ups", "string", "ups", "int"),
    ("downs", "string", "downs", "int")
]
ChangeSchema_node1720499197675 = ApplyMapping.apply(
    frame=DropFields_node1720497770957,
    mappings=mapped_fields,
    transformation_ctx="ChangeSchema_node1720499197675"
)

# Write the transformed dynamic frame to S3 as CSV files with comma delimiter
AmazonS3_node1720500509234 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1720499197675,
    connection_type="s3",
    format="csv",
    format_options={"writeHeader": True, "separator": ","},  # Ensure comma as separator
    connection_options={"path": "s3://s3-staging-canbuddy-demo", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1720500509234"
)

# Commit the job
job.commit()
