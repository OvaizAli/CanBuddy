import praw
import prawcore
import json
import boto3
import os

def lambda_handler(event, context):
    # Environment variables
    reddit_client_id = os.environ['REDDIT_CLIENT_ID']
    reddit_client_secret = os.environ['REDDIT_CLIENT_SECRET']
    reddit_user_agent = os.environ['REDDIT_USER_AGENT']
    reddit_refresh_token = os.environ['REDDIT_REFRESH_TOKEN']
    aws_region_name = os.environ['AWS_REGION_NAME']
    s3_bucket_name = os.environ['S3_BUCKET_NAME']

    # Initialize Reddit instance
    reddit = praw.Reddit(client_id=reddit_client_id,
                         client_secret=reddit_client_secret,
                         user_agent=reddit_user_agent,
                         refresh_token=reddit_refresh_token)

    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=aws_region_name)

    subreddit_names = [
        "Canada",
        "ImmigrationCanada",
        "CanadaPolitics",
        "onguardforthee",
        "metacanada",
        "CanadaHousing2",
        "Canada_sub",
        "PersonalFinanceCanada",
        "CostcoCanada",
        "CanadaUniversities",
        "MortgagesCanada",
        "AskACanadian",
        "ShopCanada",
        "CanadaPublicServants",
        "CanadaJobs",
        "canadahousing"
    ]

    results = []

    for subreddit_name in subreddit_names:
        try:
            subreddit = reddit.subreddit(subreddit_name)

            subreddit_data = []

            # Fetching posts from subreddit
            for submission in subreddit.new(limit=10): 
                reddit_data = {
                    'subreddit': subreddit_name,
                    'title': submission.title,
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'created_utc': submission.created_utc,
                    'author': submission.author.name if submission.author else None,
                    'url': submission.url,
                    'permalink': submission.permalink,
                    'upvote_ratio': submission.upvote_ratio,
                    'thumbnail': submission.thumbnail,
                    'subreddit_subscribers': submission.subreddit.subscribers,
                    'link_flair_text': submission.link_flair_text,
                    'is_video': submission.is_video,
                    'domain': submission.domain,
                    'author_fullname': submission.author_fullname,
                    'link_flair_richtext': submission.link_flair_richtext,
                    'pwls': submission.pwls,
                    'gilded': submission.gilded,
                    'thumbnail_height': submission.thumbnail_height,
                    'thumbnail_width': submission.thumbnail_width,
                    'total_awards_received': submission.total_awards_received,
                    'is_original_content': submission.is_original_content,
                    'link_flair_type': submission.link_flair_type,
                    'allow_live_comments': submission.allow_live_comments,
                    'is_self': submission.is_self,
                    'ups': submission.ups,
                    'downs': submission.downs
                }

                subreddit_data.append(reddit_data)

            # Write subreddit data to S3
            file_key = f"{subreddit_name}/{subreddit_name}_data.json"
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=file_key,
                Body=json.dumps(subreddit_data).encode('utf-8')
            )

            results.append({"subreddit": subreddit_name, "status": "success", "file_key": file_key})

        except prawcore.exceptions.Forbidden as e:
            results.append({"subreddit": subreddit_name, "status": "error", "message": f"Access to r/{subreddit_name} is Forbidden: {e}"})
        except Exception as e:
            results.append({"subreddit": subreddit_name, "status": "error", "message": f"Error accessing r/{subreddit_name}: {e}"})

    # Check if there are any errors
    all_successful = all(result["status"] == "success" for result in results)
    status_code = 200 if all_successful else 500

    return {
        'statusCode': status_code,
        'body': json.dumps({"results": results})
    }
