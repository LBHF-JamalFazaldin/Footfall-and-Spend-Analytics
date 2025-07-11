import os
import pandas as pd
import inspect as insp
from sqlalchemy import create_engine
from IPython.display import display as original_display

# Database credentials
db_host = 'LBHHLWSQL0001.lbhf.gov.uk'
db_port = '1433'
db_name = 'IA_ODS'

# Create the connection string for SQL Server using pyodbc with Windows Authentication
connection_string = f'mssql+pyodbc://@{db_host}:{db_port}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes'

# Create the database engine
engine = create_engine(connection_string)

def clean_label(label):
    """
    Cleans a label string by replacing underscores with spaces and converting to title case.

    Args:
        label (str): The label string to clean.

    Returns:
        str: The cleaned label string.
    """
    try:
        return label.replace('_', ' ').title()
    except AttributeError as e:
        print(f'Error cleaning label: {e}')
        return label

def get_var_name(var):
    """
    Retrieves the variable name of a DataFrame from the global scope.

    Args:
        var (object): The variable to find the name for.

    Returns:
        str or None: The variable name if found, else None.
    """
    try:
        for name, value in globals().items():
            if value is var:
                return name
    except Exception as e:
        print(f'Error getting variable name: {e}')
    return None

def header_list(df):
    """
    Returns a DataFrame containing the column headers of the input DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to extract headers from.

    Returns:
        pd.DataFrame: DataFrame containing the headers.
    """
    try:
        df_list_ = df.copy()
        df_list = df_list_.columns.tolist()
        df_list = pd.DataFrame(df_list)
        new_header = df_list.iloc[0]  # Get the first row for the header
        df_list = df_list[1:]  # Take the data less the header row
        df_list.columns = new_header  # Set the header row as the df header
        df_list.reset_index(drop=True, inplace=True)  # Reset index
        return df_list
    except Exception as e:
        print(f'Error creating header list: {e}')
        return pd.DataFrame()

def read_directory(directory=False):
    """
    Lists files in the specified directory or current working directory if none is provided.

    Args:
        directory (str, optional): The directory to read. Defaults to current working directory.

    Returns:
        None
    """
    if directory == False:
        directory = os.getcwd()
        
    files = os.listdir(directory)
    
    if directory == os.getcwd():
        print(f"Your Current Directory is: {directory}")
    else:
        print(f"Directory being read is: {directory}")

    print("Files in: %s\n" % (files))

def display(df):
    """
    Displays a DataFrame with its variable name if available.

    Args:
        df (pd.DataFrame): The DataFrame to display.

    Returns:
        None
    """
    try:
        frame = insp.currentframe().f_back
        name = "Unnamed DataFrame"
        for var_name, var_value in frame.f_locals.items():
            if var_value is df:
                name = var_name
                break
        if name not in {'df', 'Unnamed DataFrame', 'unique_counts'}:
            print(f"DataFrame: {name}")
        original_display(df)
    except Exception as e:
        print(f'Error displaying DataFrame: {e}')

def unique_values(df, display_df=True):
    """
    Returns a DataFrame containing unique values for each column in the input DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to analyze.
        display_df (bool, optional): Whether to display the DataFrame. Defaults to True.

    Returns:
        pd.DataFrame: DataFrame of unique values per column.
    """
    try:
        unique_values = {col: df[col].unique() for col in df.columns}
        max_length = max(len(values) for values in unique_values.values())
        unique_df_data = {}
        for col, values in unique_values.items():
            unique_df_data[col] = list(values) + [None] * (max_length - len(values))
        unique_df = pd.DataFrame(unique_df_data)
        if display_df:
            pd.set_option('display.max_rows', None)
            display(unique_df.head(100))
            pd.reset_option('display.max_rows')
        return unique_df
    except Exception as e:
        print(f'Error extracting unique values: {e}')
        return pd.DataFrame()

def validate_data(df, show_counts=True):
    """
    Validates a DataFrame by displaying its name, snapshot, unique value counts, duplicate rows, info, and summary statistics.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        show_counts (bool, optional): Whether to show counts in info. Defaults to True.

    Returns:
        None
    """
    try:
        df_name = get_var_name(df)
        print(f'#########################################################################################################################################################################################\nDataFrame: {df_name}')
        
        # Snapshot the dataset
        display(df)
        
        # Check for unique values
        unique_counts = pd.DataFrame(df.nunique())
        unique_counts = unique_counts.reset_index().rename(columns={0:'No. of Unique Values', 'index':'Field Name'})
        print("Unique values per field:")
        pd.set_option('display.max_rows', None)
        display(unique_counts)
        pd.reset_option('display.max_rows')
        
        # Checking for duplicates
        duplicate_count = df.duplicated().sum()
        print("\nNumber of duplicate rows:")
        print(duplicate_count,'\n')
        info = df.info(show_counts=show_counts)
        display(info)
        # Summary stats
        print("\nSummary statistics:")
        display(df.describe())
        print('End of data validation\n#########################################################################################################################################################################################\n')
    except Exception as e:
        print(f'Error validating data: {e}')

def query_data(schema, data):
    """
    Queries a SQL Server database table and returns the result as a DataFrame.

    Args:
        schema (str): The schema name.
        data (str): The table name.

    Returns:
        pd.DataFrame: The queried data as a DataFrame.
    """
    try:
        query = f'SELECT * FROM [{schema}].[{data}]'
        df = pd.read_sql(query, engine)
        print(f'Successfully imported {data}')
        return df
    except Exception as e:
        print(f'Error querying data: {e}')
        return pd.DataFrame()

def export_to_csv(df, **kwargs):
    """
    Exports a DataFrame to a CSV file in the specified directory.

    Args:
        df (pd.DataFrame): The DataFrame to export.
        directory (str, optional): The directory to save the CSV. Defaults to a preset path.
        df_name (str, optional): The name for the CSV file. Defaults to the variable name.

    Returns:
        None
    """
    try:
        directory = kwargs.get('directory',r"C:\Users\jf79\OneDrive - Office Shared Service\Documents\H&F Analysis\Python CSV Repositry")
        df_name = kwargs.get('df_name',get_var_name(df))
        if not isinstance(df_name, str) or df_name == '_':
                df_name = input('Dataframe not found in global variables. Please enter a name for the DataFrame: ')

        file_path = f'{directory}\\{df_name}.csv'

        print(f'Exproting {df_name} to CSV...\n@ {file_path}\n')
        df.to_csv(file_path, index=False)
        print(f'Successfully exported {df_name} to CSV')
    except Exception as e:
        print(f'Error exporting to CSV: {e}')