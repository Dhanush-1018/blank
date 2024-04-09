import os
import pip

try:
    from github import Github
except:
    pip.main(['install', 'PyGithub'])
    
try:   
    import requests
except:
    pip.main(['install', 'requests'])


def github_to_local(repo_link,  local_path,token=None, subfolder_name=None):
    """
    Downloads all files from a GitHub repository to a local path.

    Args:
        repo_link: The URL of the GitHub repository.
        token: A personal access token with sufficient permissions to access the repository.
        local_path: The local path where the downloaded files will be saved.
        subfolder_name: (Optional) The name of the subfolder within the repository to download.
            If not provided, the entire repository will be downloaded.
    """

    # Ensure the local path exists
    if not os.path.exists(local_path):
        raise ValueError(f"Local path '{local_path}' does not exist.")

    try:
        if subfolder_name:
            if token:
                download_subfolder(repo_link, local_path, subfolder_name, token)
            else:
                download_subfolder(repo_link, local_path, subfolder_name)        
        else:
            if token:
                download_repository(repo_link, local_path, token)
            else:
                download_repository(repo_link, local_path)
                
    except Exception as e:
        print(f"An error occurred during download: {e}")


def download_repository(repo_link, local_path, token = None):
    """
    Downloads the entire repository to the specified local path.
    """
    
    if token: 
        g = Github(token)
    else:
        g = Github()
    owner, repo_name = repo_link.split("/")[-2:]
    repo = g.get_repo(f"{owner}/{repo_name}")

    # Define download_dir function within its scope
    def download_dir(contents, local_dir):
        # Create the local directory if it doesn't exist
        os.makedirs(os.path.join(local_path, local_dir), exist_ok=True)

        # Download each file or recurse into subfolders
        for content in contents:
            if content.type == "file":
                download_url = content.download_url
                local_file_path = os.path.join(local_path, local_dir, content.name)

                # Download the file and handle potential exceptions
                try:
                    with open(local_file_path, "wb") as f:
                        f.write(requests.get(download_url).content)
                    print(f"Downloaded: {local_file_path}")
                except Exception as e:
                    print(f"Error downloading {content.name}: {e}")
            elif content.type == "dir":
                # Recursively download subfolder content
                download_dir(repo.get_contents(content.path), os.path.join(local_dir, content.name))

    # Download from the root of the repository
    download_dir(repo.get_contents(""), local_path)


def download_subfolder(repo_link, local_path, subfolder_name=None, token=None):
    """
    Downloads all files recursively from a subfolder within a GitHub repository to a local path.
    
    Args:
        repo_link: The URL of the GitHub repository.
        token: A personal access token with sufficient permissions to access the repository.
        local_path: The local path where the downloaded files will be saved.
        subfolder_name: The name of the subfolder within the repository to download.
    """

    # Create a Github object using the provided token
    if token :
        g = Github(token)
    else:
        g = Github()

    # Extract the owner and repository name from the URL
    owner, repo_name = repo_link.split("/")[-2:]

    # Get the repository object
    repo = g.get_repo(f"{owner}/{repo_name}")

    def download_dir(contents, local_dir):
        # Create the local directory if it doesn't exist
        os.makedirs(os.path.join(local_path, local_dir), exist_ok=True)

        # Download each file or recurse into subfolders
        for content in contents:
            if content.type == "file":
                download_url = content.download_url
                local_file_path = os.path.join(local_path, local_dir, content.name)

                # Download the file and handle potential exceptions
                try:
                    with open(local_file_path, "wb") as f:
                        f.write(requests.get(download_url).content)
                    print(f"Downloaded: {local_file_path}")
                except Exception as e:
                    print(f"Error downloading {content.name}: {e}")
            elif content.type == "dir":
                # Recursively download subfolder content
                download_dir(repo.get_contents(content.path), os.path.join(local_dir, content.name))

    # Download the specified subfolder and its contents recursively..
    download_dir(repo.get_contents(subfolder_name), subfolder_name)


