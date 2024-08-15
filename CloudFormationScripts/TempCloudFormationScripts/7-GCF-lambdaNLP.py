import functions_framework
import pandas as pd
from io import StringIO
from google.cloud import language_v1
from flask import Response
from google.cloud import storage

@functions_framework.http
def process_file(request):
    """HTTP Cloud Function to process a CSV file, analyze content, return the modified file, and upload it to Google Cloud Storage."""
    try:
        # Read the file from the request
        file = request.files['file']
        file_contents = file.read().decode('utf-8')
        
        # Parse the CSV data
        df = pd.read_csv(StringIO(file_contents))
        
        # Check if required columns exist
        if 'title' not in df.columns or 'selftext' not in df.columns:
            return Response(
                'CSV file must contain "title" and "selftext" columns.',
                status=400
            )
        
        # Fill empty 'selftext' fields with empty string and concatenate 'title' and 'selftext'
        df['selftext'] = df['selftext'].fillna("")
        df['content'] = df['title'].astype(str) + " " + df['selftext'].astype(str)
        
        # Perform NLP analysis to determine sentiment and extract key phrases
        nlp_results = df['content'].apply(analyze_content)
        
        # Concatenate the NLP results with the original dataframe
        df = pd.concat([df, nlp_results], axis=1)
        
        # Convert the dataframe back to CSV
        output = StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        # Upload the modified file to Google Cloud Storage
        client = storage.Client()
        bucket = client.bucket('cs-nlp-processed')
        blob = bucket.blob('processed_data.csv')
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        
        # Return the modified CSV file
        return Response(
            output.getvalue(),
            status=200,
            mimetype='text/csv'
        )
    
    except Exception as e:
        return Response(
            f'Error processing the file: {str(e)}',
            status=500
        )

def analyze_content(content):
    """Function to perform sentiment analysis and extract key phrases using Google Cloud Natural Language API."""
    try:
        # Initialize Google Cloud Natural Language API client
        client = language_v1.LanguageServiceClient()

        # Create a document object
        document = language_v1.Document(content=content, type_=language_v1.Document.Type.PLAIN_TEXT)

        # Analyze sentiment
        sentiment_response = client.analyze_sentiment(request={'document': document})
        sentiment = sentiment_response.document_sentiment

        # Determine sentiment category
        if sentiment.score > 0:
            sentiment_category = 'positive'
        elif sentiment.score < 0:
            sentiment_category = 'negative'
        else:
            sentiment_category = 'neutral'

        # Analyze syntax to extract key phrases
        syntax_response = client.analyze_syntax(request={'document': document})
        key_phrases = [token.text.content for token in syntax_response.tokens if token.part_of_speech.tag in [language_v1.PartOfSpeech.Tag.NOUN, language_v1.PartOfSpeech.Tag.VERB]]

        # Return the results as a Series (which will be converted to DataFrame columns)
        return pd.Series({
            'sentiment': sentiment_category,
            'sentiment_score': sentiment.score,
            'sentiment_magnitude': sentiment.magnitude,
            'key_phrases': ', '.join(key_phrases)
        })
    
    except Exception as e:
        print("Error in NLP analysis:", str(e))
        # Return default values in case of error
        return pd.Series({
            'sentiment': 'error',
            'sentiment_score': 0,
            'sentiment_magnitude': 0,
            'key_phrases': ''
        })
