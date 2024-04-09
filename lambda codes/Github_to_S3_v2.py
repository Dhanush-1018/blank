import os
import boto3
from github import Github
from requests.exceptions import RequestException

# AWS S3 configuration
AWS_BUCKET_NAME = input("Enter the name of the S3 bucket: ")
AWS_ACCESS_KEY_ID = input("Enter your AWS Access Key ID: ")
AWS_SECRET_ACCESS_KEY =input("Enter your AWS Secret Access Key: ")

def move_to_s3(repo_link, subfolder_name=None, github_token=None):
    """
    Moves files from a GitHub repository or a specific folder to an S3 bucket.

    Args:
        repo_link (str): The URL of the GitHub repository.
        subfolder_name (str, optional): The name of the subfolder within the repository to move. Defaults to None (entire repo).
        github_token (str, optional): A personal access token for GitHub if the repository is private. Defaults to None.
    """
    try:
        # Connect to GitHub
        if github_token:
            g = Github(github_token)
        else:
            g = Github()

        # Get repository
        owner, repo_name = repo_link.split("/")[-2:]
        repo = g.get_repo(f"{owner}/{repo_name}")

        # Determine whether to move the entire repository or a specific folder
        if subfolder_name:
            contents = repo.get_contents(subfolder_name)
        else:
            contents = repo.get_contents("")

        # Connect to S3
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # Move files to S3
        for content in contents:
            try:
                if content.type == "file":
                    file_content = content.decoded_content
                    s3.put_object(Bucket=AWS_BUCKET_NAME, Key=content.path, Body=file_content)
                    print(f"Moved {content.path} to S3 successfully!")
                elif content.type == "dir":
                    # Recursively move files in subdirectories
                    move_to_s3(repo_link, content.path, github_token)
                    print(f"Moved contents of {content.path} to S3 successfully!")
            except Exception as e:
                print(f"Error moving {content.path}: {e}")

    except RequestException as e:
        print(f"Error: Request Exception - {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

#github_url_format: https://github.com/{owner_name}/{rep0_name}

if __name__ == "__main__":
    repo_link = input("Enter the URL of the GitHub repository: ")
    subfolder_name = input("Enter the name of the subfolder (leave empty for entire repository): ")
    github_token = input("Enter your GitHub personal access token (leave empty if repository is public): ")
    
    move_to_s3(repo_link, subfolder_name, github_token)






