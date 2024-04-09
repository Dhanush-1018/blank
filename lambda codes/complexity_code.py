import re
import os
import os.path
import pip
import working_github_subfolder3
import boto3
try:
    import pandas as pd
except ImportError:
    pip.main(['install', 'Pandas'])
    import pandas as pd




try:
    git_flag = input("Where is you data - (Github, S3, Local)): ").lower()
except KeyboardInterrupt:
    print("\nProgram terminated by user.")
    exit()

if git_flag == 'github':
    print("\nPlease fill below details; refer to https://github.com/m-jatin/complexity/tree/main/Input_and_Output for more information\n")
    try:
        repo_name = input("Please provide repository name (e.g., complexity): ")
        owner_name = input("Please provide owner name (e.g., m-jatin): ")
        repo_link = f"https://api.github.com/repos/{owner_name}/{repo_name}"
        token = input("Please provide token (None for public repos): ")
        token = None if token == 'None' else token
        subfolder_name = input("Please provide subfolder name containing 'Lookup_Table' folder (e.g., Input_and_Output) \n (None if Lookup_Table folder is available in repository itself): ")

        local_path = os.getcwd()
        
        print(f"\nOutput will be available at: {local_path}")
        loc_flag = input("Do you want to change the output path (Yes/No): ").lower()
        if loc_flag == "yes":
            local_path = input("Please provide the path (Windows format): ")
            local_path = os.path.normpath(local_path)

        file_extension = f'.{input("Please provide file extension (e.g., txt): ")}' 
        working_github_subfolder3.github_to_local(repo_link, local_path, token, subfolder_name)

        if subfolder_name:
            Input_and_Ouput_Folder_Location = os.path.join(local_path, subfolder_name)
        else:
            Input_and_Ouput_Folder_Location = local_path

    except Exception as e:
        print(f"An error occurred: {e}")

elif git_flag =='local':
    try:
        Input_and_Ouput_Folder_Location = input("Please provide the input path (Windows format): ")
        Input_and_Ouput_Folder_Location = os.path.normpath(Input_and_Ouput_Folder_Location)
        file_extension = f'.{input("Please provide file extension (e.g., txt): ")}'
        local_path = os.getcwd()
        print(f"\nOutput will be available at: {local_path}")
        loc_flag = input("Do you want to change the output path (Yes/No): ").lower()
        if loc_flag == "yes":
            local_path = input("Please provide the path (Windows format): ")
            local_path = os.path.normpath(local_path)
        else:
            local_path = os.path.join(
                Input_and_Ouput_Folder_Location, 'Output')
        if os.path.exists(local_path) == False:
            os.mkdir(local_path)                        

    except Exception as e:
        print(f"An error occurred: {e}")
                 
    
elif git_flag =='s3':

    access_key = input("Enter your AWS Access Key ID: ")
    secret_key = input("Enter your AWS Secret Access Key: ")
    bucket_name = input("Enter the name of the S3 bucket: ")
    output_bucket_name = input("Enter the name of the output S3 bucket: ")
    file_extension = f'.{input("Please provide file extension (e.g., txt): ")}'
    s3_folder_name = input("Enter the name of the folder: ") 
    lookup_file_key = input("Enter your lookup table Key ID: ")
    s3_bucket_folder_location = f"s3://{bucket_name}/{s3_folder_name}"
    
    
    local_path = os.getcwd()
    print(f"\nOutput will be available at: {local_path}")
    loc_flag = input("Do you want to change the output path (Yes/No): ").lower()
    if loc_flag == "yes":
        local_path = input("Please provide the path (Windows format): ")
        local_path = os.path.normpath(local_path)
    else:
        local_path = os.path.join(
            local_path, 'Output')
    if os.path.exists(local_path) == False:
        os.mkdir(local_path)
else:
    print('Code is limited to Github,S3 and Local only')
        



def count_subqueries_in_sql(query):
    """Counts the number of subqueries (nested SELECT statements) in a SQL query string.

    Args:
        query (str): The SQL query string to analyze.

    Returns:
        int: The number of subqueries found in the query.
    """

    subquery_pattern = r"""\b(?:SELECT\b|\bFROM\b)\s*\([^;)]*\)\b"""  # Improved pattern for capturing subqueries
    subqueries = re.findall(subquery_pattern, query, re.IGNORECASE)
    return len(subqueries)


def count_statements(file_path):
    """Counts the occurrences of SQL statements and subqueries in a file.

    Args:
        file_path (str): The path to the file containing SQL statements.

    Returns:
        dict: A dictionary containing counts for each statement and subquery.

    Raises:
        FileNotFoundError: If the file is not found.
    """

    total_lines = 0
    statements = [
        "create volatile", "create table", "insert", "update", "merge", "delete", "select", "export", "import",
        "with", "rank", "dense_rank", "add_months", "months_between", "last_day", "next_day", "cast", "coalesce",
        "zeroiffull", "min", "max", "sum", "avg", "count", "substr", "length", "upper", "lower", "trim", "like",
        "case", "distinct", "row_number", "top", "qualify", "locate", "grouping", "group by", "order by", "join",
        "where", "del", "sel","alter"
    ]
    #initilizing with 0 for all statements
    statement_counts = dict.fromkeys(
        statements + ['total_lines', 'subquery_count'], 0)

    try:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read().lower()

            lines = content.splitlines()
            content = "\n".join(
                line for line in lines if not line.startswith("--"))

            for statement in statements:
                pattern = r"\b" + statement + r"\b"             
                statement_counts[statement] = len(re.findall(pattern, content))
                
            statement_counts['total_lines'] = sum(
                1 for line in content.splitlines() if line.strip())
            statement_counts['subquery_count'] = count_subqueries_in_sql(
                content)

    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

    return statement_counts

def count_statements_s3(content):
    """Counts the occurrences of SQL statements and subqueries in a file.

    Args:
        content (str): The content of the SQL file as a string.

    Returns:
        dict: A dictionary containing counts for each statement and subquery.
    """
    total_lines = 0
    statements = [
        "create volatile", "create table", "insert", "update", "merge", "delete", "select", "export", "import",
        "with", "rank", "dense_rank", "add_months", "months_between", "last_day", "next_day", "cast", "coalesce",
        "zeroiffull", "min", "max", "sum", "avg", "count", "substr", "length", "upper", "lower", "trim", "like",
        "case", "distinct", "row_number", "top", "qualify", "locate", "grouping", "group by", "order by", "join",
        "where", "del", "sel","alter"
    ]
    statement_counts = dict.fromkeys(statements + ['total_lines', 'subquery_count'], 0)

    try:
        lines = content.lower().splitlines()
        content = "\n".join(line for line in lines if not line.startswith("--"))

        for statement in statements:
            pattern = r"\b" + statement + r"\b"
            statement_counts[statement] = len(re.findall(pattern, content))

        statement_counts['total_lines'] = sum(1 for line in content.splitlines() if line.strip())
        statement_counts['subquery_count'] = count_subqueries_in_sql(content)

    except Exception as e:
        print(f"Error processing file content: {str(e)}")

    return statement_counts


def process_folder_count_statement(folder_path,  path):
    """Counts the occurrences of SQL statements and subqueries in each file in a folder.

    Args:
        folder_path (str): The path to the folder containing SQL script files.

    Returns:
        dict: A nested dictionary containing counts for each statement and subquery in each file.

    Raises:
        FileNotFoundError: If the folder is not found.
    """

    count_statement_results = {}
    
    

    try:
        
        
        if folder_path.startswith("s3://"):
            # Process files from S3 bucket folder
            bucket_name, prefix = folder_path.split('/')[2], '/'.join(folder_path.split('/')[3:])
            s3 = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            for obj in response.get('Contents', []):               
                file_path = obj['Key']
                if file_path.endswith(file_extension):
                    s3_file_content = s3.get_object(Bucket=bucket_name, Key=file_path)['Body'].read().decode('utf-8')
                    count_statement_results[f"s3://{bucket_name}/{file_path}"] = count_statements_s3(content = s3_file_content)                
            dataframe_list = []
            for key, value in count_statement_results.items():
                # print(f'key = {key}')
                # print(f'Input_and_Ouput_Folder_Location = {Input_and_Ouput_Folder_Location}')
                value["01. Filepath"] = key[len(path) + 1:]
                dataframe_list.append(value)
    
            df = pd.DataFrame(dataframe_list)
    
            #bringing filename to first
            last_column = df.columns[-1]
            df = df[[last_column] + list(df.columns[:-1])]
    
            # Export the DataFrame to a CSV file
            # df.transpose().to_csv(output_file_name, header=False)
            # df.to_csv(output_file_name, index=False)
            return df.transpose()                    
                
                
                
        else:
            if not os.path.isdir(folder_path):
                raise FileNotFoundError(f"Folder not found: {folder_path}")
    
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    if file_name.endswith(file_extension):  # Handle only .txt files
                        file_path = os.path.join(root, file_name)
                        count_statement_results[file_path] = count_statements(
                            file_path)
                  

            dataframe_list = []
            for key, value in count_statement_results.items():
                # print(f'key = {key}')
                # print(f'Input_and_Ouput_Folder_Location = {Input_and_Ouput_Folder_Location}')
                value["01. Filepath"] = key
                dataframe_list.append(value)
    
            df = pd.DataFrame(dataframe_list)
    
            #bringing filename to first
            last_column = df.columns[-1]
            df = df[[last_column] + list(df.columns[:-1])]
    
            # Export the DataFrame to a CSV file
            # df.transpose().to_csv(output_file_name, header=False)
            # df.to_csv(output_file_name, index=False)
            return df.transpose()

    except Exception as e:
        print(f"Error processing folder {folder_path}: {str(e)}")


# =============================================================================
# Modify below as required
# =============================================================================

#The folder that conatins the folders ETL Scripts and Workflows


Output_Dir =local_path

if git_flag != 's3':

    Lookup_Dir = os.path.join(
        Input_and_Ouput_Folder_Location, "Lookup_Table")
    #list of all subfolders present
    subfolders_all = [f.name for f in os.scandir(Input_and_Ouput_Folder_Location) if f.is_dir()]
    subfolders = []
    for folder_name in subfolders_all:
        folder_path = os.path.join(Input_and_Ouput_Folder_Location, folder_name)
        txt_files = [file.name for file in os.scandir(folder_path) if file.is_file() and file.name.endswith(file_extension)]
        if txt_files:
            subfolders.append(folder_name)
            print(f'{folder_name} contains {len(txt_files)} {file_extension} files')
    
    
else:
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    # S3 bucket name and prefix (folder path)
    prefix = f'{s3_folder_name}/'
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Extract subfolders with TXT files
    subfolders_with_txt = [
        obj['Key'].split('/')[1]  # Assuming subfolder structure: bucket_name/folder_name/file_name.txt
        for obj in response.get('Contents', [])
        if obj['Key'].endswith(file_extension) and '/' in obj['Key']
    ]
    from collections import Counter
    
    # Count the occurrences of each subfolder
    subfolder_counts = Counter(subfolders_with_txt)
    
    # Print the number of text files for each subfolder
    for subfolder, count in subfolder_counts.items():
        print(f'{subfolder} contains {count} {file_extension} files')
            
    subfolders = list(set(subfolders_with_txt))




try:
    subfolders.remove('Lookup_Table')
except:
    pass

try:
    subfolders.remove('Output')
except:
    pass



if git_flag != 's3':
    subfolder_path ={}
    for x in subfolders:
        subfolder_path[x] = os.path.join(
            Input_and_Ouput_Folder_Location, x)
else:
    subfolder_path ={}
    for x in subfolders:
        subfolder_path[x] = f"s3://{bucket_name}/{s3_folder_name}/{x}"
    
    

if os.path.exists(Output_Dir) == False:
    os.mkdir(Output_Dir)


# Occurence_Count_Dir = os.path.join(Output_Dir, "1.Occurence_Counts")
# if os.path.exists(Occurence_Count_Dir) == False:
#     os.mkdir(Occurence_Count_Dir)

Multiplied_Dir = Output_Dir
if os.path.exists(Multiplied_Dir) == False:
    os.mkdir(Multiplied_Dir)

# etl_output = os.path.join(Occurence_Count_Dir, "etl_output.csv")
# workflows_output = os.path.join(Occurence_Count_Dir, "workflow_output.csv")

#Delete if exists
# if os.path.exists(etl_output):
#     os.remove(etl_output)
# if os.path.exists(workflows_output):
#     os.remove(workflows_output)

# Count number of files in below folder - ETL_Scripts and Workflows


#key is subfolder_name and path is value
summary={}
for key,value in subfolder_path.items():
    summary[key] =  process_folder_count_statement(
        value,  path=value)




#******************************************************************************************************************




# # =============================================================================
# # Assigning Scores to each file
# # =============================================================================


if git_flag != 's3':
    try:
        lookup_df = pd.read_csv(os.path.join(
            Lookup_Dir, 'lookup_scores.csv'), index_col='Statement')
    except Exception as e:
        print(f"Error reading lookup_scores.csv: {str(e)}")
        lookup_df = pd.DataFrame()
else:
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        obj = s3.get_object(Bucket=bucket_name, Key=lookup_file_key)
        lookup_df = pd.read_csv(obj['Body'], index_col='Statement')
    except Exception as e:
        print(f"Error loading lookup_scores.csv from s3: {str(e)}")
                
def assign_score(df_name):
    try:
        df_name.index = df_name.index.str.strip()
        lookup_df.index = lookup_df.index.str.strip()
        statement_with_lookup = df_name.join(lookup_df, how='left')

        statement_with_lookup_copy = statement_with_lookup.copy()

        first_row = statement_with_lookup.iloc[0]
        statement_with_lookup = statement_with_lookup.iloc[1:]

        for col in statement_with_lookup.columns:
            try:
                statement_with_lookup[col] = pd.to_numeric(
                    statement_with_lookup[col])
            except Exception as e:
                print(e)

        statement_with_lookup['Score'] = statement_with_lookup['Score'].astype(
            float)
        column_to_multiply = statement_with_lookup['Score']

        for col in statement_with_lookup.columns:
            if col != column_to_multiply.name:
                statement_with_lookup[col] = statement_with_lookup[col] * column_to_multiply * 0.1

        statement_with_lookup_copy.loc['02. Total Score'] = statement_with_lookup.sum()

        statement_with_lookup = statement_with_lookup_copy

        replacements = {'total_lines': '03. Total_Lines',
                 'subquery_count': '04. Subquery_Count',
                 'create table': '05. Create Table',
                 'create volatile': '06. Create Volatile',
                 'insert': '07. Insert',
                 'update': '08. Update',
                 'alter': '09. Alter',
                 'del': '10. Del',
                 'delete': '11. Delete',
                 'merge': '12. Merge',
                 'avg': '13. Avg',
                 'count': '14. Count',
                 'max': '15. Max',
                 'min': '16. Min',
                 'substr': '17. Substr',
                 'sum': '18. Sum',
                 'select': '19. Select',
                 'sel': '20. Sel',
                 'cast': '21. Cast',
                 'coalesce': '22. Coalesce',
                 'dense_rank': '23. Dense_Rank',
                 'distinct': '24. Distinct',
                 'group by': '25. Group By',
                 'grouping': '26. Grouping',
                 'last_day': '27. Last_Day',
                 'length': '28. Length',
                 'like': '29. Like',
                 'locate': '30. Locate',
                 'lower': '31. Lower',
                 'months_between': '32. Months_Between',
                 'next_day': '33. Next_Day',
                 'order by': '34. Order By',
                 'rank': '35. Rank',
                 'row_number': '36. Row_Number',
                 'trim': '37. Trim',
                 'upper': '38. Upper',
                 'add_months': '39. Add_months',
                 'export': '40. Export',
                 'import': '41. Import',
                 'join': '42. Join',
                 'qualify': '43. Qualify',
                 'case': '44. Case',
                 'top': '45. Top',
                 'where': '46. Where',
                 'with': '47. With',
                 'zeroiffull': '48. Zeroiffull'}

        statement_with_lookup = statement_with_lookup.rename(replacements, axis=0)
        statement_with_lookup = statement_with_lookup.sort_index()

        inverse_replacements = {
            '01. Filepath': 'Filepath',
            '02. Total Score': 'Total Score',
            '03. Total_Lines': 'Total_Lines',
            '04. Subquery_Count': 'Subquery_Count',
            '05. Create Table': 'Create Table',
            '06. Create Volatile': 'Create Volatile',
            '07. Insert': 'Insert',
            '08. Update': 'Update',
            '09. Alter': 'Alter',
            '10. Del': 'Del',
            '11. Delete': 'Delete',
            '12. Merge': 'Merge',
            '13. Avg': 'Avg',
            '14. Count': 'Count',
            '15. Max': 'Max',
            '16. Min': 'Min',
            '17. Substr': 'Substr',
            '18. Sum': 'Sum',
            '19. Select': 'Select',
            '20. Sel': 'Sel',
            '21. Cast': 'Cast',
            '22. Coalesce': 'Coalesce',
            '23. Dense_Rank': 'Dense_Rank',
            '24. Distinct': 'Distinct',
            '25. Group By': 'Group By',
            '26. Grouping': 'Grouping',
            '27. Last_Day': 'Last_Day',
            '28. Length': 'Length',
            '29. Like': 'Like',
            '30. Locate': 'Locate',
            '31. Lower': 'Lower',
            '32. Months_Between': 'Months_Between',
            '33. Next_Day': 'Next_Day',
            '34. Order By': 'Order By',
            '35. Rank': 'Rank',
            '36. Row_Number': 'Row_Number',
            '37. Trim': 'Trim',
            '38. Upper': 'Upper',
            '39. Add_months': 'Add_months',
            '40. Export': 'Export',
            '41. Import': 'Import',
            '42. Join': 'Join',
            '43. Qualify': 'Qualify',
            '44. Case': 'Case',
            '45. Top': 'Top',
            '46. Where': 'Where',
            '47. With': 'With',
            '48. Zeroiffull': 'Zeroiffull'}

        statement_with_lookup = statement_with_lookup.rename(
            inverse_replacements, axis=0)

        statement_with_lookup = statement_with_lookup.transpose()
        statement_with_lookup = statement_with_lookup.sort_values(by='Total Score')
        statement_with_lookup = statement_with_lookup.drop('Score').transpose()
        return statement_with_lookup

    except Exception as e:
        print(f"Error assigning score: {str(e)}")
        return pd.DataFrame()

  

scored_df = {}
try:
    for key, value in summary.items():
        scored_df[key] = assign_score(value)
        scored_df[key].transpose().to_csv(
            (os.path.join(Multiplied_Dir, f'{key}_Analysis.csv')), index=False)
except Exception as e:
    print(f"Error processing and saving scored files: {str(e)}")

if git_flag =='s3':
    def output_to_s3(local_dir, bucket_name):
        s3 = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_path = os.path.join(bucket_name, relative_path).replace("\\", "/")  
                s3.upload_file(local_path, bucket_name, s3_path)
    try:            
     output_to_s3(local_path, output_bucket_name)
    except Exception as e:
        print(f"Error processing output to s3: {str(e)}")
    