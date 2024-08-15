import boto3
import pandas as pd
from io import BytesIO
import os
from textblob import TextBlob
import nltk

# Ensure nltk resources are downloaded
nltk.download('punkt')
nltk.download('vader_lexicon')

def analyze_sentiment(text):
    if not text:
        return "neutral"
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return "positive"
    elif analysis.sentiment.polarity < 0:
        return "negative"
    else:
        return "neutral"

# def extract_keywords(text):
#     rake = Rake()
#     rake.extract_keywords_from_text(text)
#     return ', '.join(rake.get_ranked_phrases())

def lambda_handler(event, context):
    # S3 configuration
    source_bucket_name = os.environ['SOURCE_BUCKET_NAME']
    destination_bucket_name = os.environ['DESTINATION_BUCKET_NAME']
    destination_file_key = os.environ['DESTINATION_FILE_KEY']
    
    s3_client = boto3.client('s3')
    
    try:
        # List all objects in the source bucket
        objects = []
        response = s3_client.list_objects_v2(Bucket=source_bucket_name)
        
        for obj in response.get('Contents', []):
            objects.append(obj['Key'])
        
        # Read and concatenate all CSV-like files
        if objects:
            dataframes = []
            for file_key in objects:
                obj = s3_client.get_object(Bucket=source_bucket_name, Key=file_key)
                df = pd.read_csv(BytesIO(obj['Body'].read()))
                
                # Add sentiment analysis
                df['sentiment'] = df.apply(lambda row: analyze_sentiment(row['title'] + ' ' + row['selftext']), axis=1)
                
                # Add keyword extraction
                # df['keywords'] = df.apply(lambda row: extract_keywords(row['title'] + ' ' + row['selftext']), axis=1)
                
                dataframes.append(df)
            
            # Concatenate all dataframes
            final_df = pd.concat(dataframes, ignore_index=True)
            
            # Convert final DataFrame to CSV
            csv_buffer = BytesIO()
            final_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Upload the CSV file to S3
            s3_client.upload_fileobj(csv_buffer, destination_bucket_name, destination_file_key)
            
            return {
                'statusCode': 200,
                'body': 'CSV-like files merged and uploaded as CSV successfully.'
            }
        else:
            return {
                'statusCode': 200,
                'body': 'No CSV-like files found in the source bucket.'
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error merging and uploading CSV-like files: {str(e)}"
        }
# EOF

# # Zip the directory contents
# cd ..
# zip -r lambda_code.zip lambda_code
