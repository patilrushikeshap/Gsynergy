import boto3
import gzip 
from io import BytesIO
import pandas as pd
import os


#define the s3 bucket name and directory
bucket_name = 'enter_bucket_name'
directory = 'enter_directory'

#Provide aws_access_key_id, aws_secret_access_key to establish the s3 connection 
s3 = boto3.client('s3', aws_access_key_id='enter_aws_access_key', aws_secret_access_key='enter_aws_secret_access_key', region_name = 'enter_region_name')
response = s3.list_objects_v2(Bucket = bucket_name,Prefix=directory)

#read gzipped_file_from_s3
def read_gzipped_file_from_s3(file_key):
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    with gzip.open(BytesIO(obj['Body'].read()), 'rt') as f:
        df = pd.read_csv(f, delimiter= '|', low_memory=False)
    return df

data = {}
file_keys = [obj['Key'] for obj in response['Contents']if obj['Key'].endswith('.gz')]


#extracting each file data from s3
for file_key in file_keys:
    table_name = os.path.basename(file_key).split('.')[0]
    data[table_name] = read_gzipped_file_from_s3(file_key)

#function to check for check_non_unique_primary_key value in the dataframe 
def check_non_unique_primary_key(df, table_name):
    primary_key = df.columns[0]
    if df.duplicated(primary_key).any():
        print("length of my original dataframe rows",len(df))
        print(f"warning: Non_unique primary key values found in {table_name}")
        duplicated_rows = df[df.duplicated(primary_key, keep = False)]
        print("length of duplicated rows values:- ", len(duplicated_rows))
        df.drop_duplicates(subset = primary_key, inplace = True)
    else:
        print(f"No non_unique primary key values found in {table_name}")
    return df

#function to fill null values
def fill_null_with_zero(df):
    return df.fillna(0)

#perform basic checks
def perform_basic_checks(df, table_name):
    global null_found 
    null_found = False
    print(f'performing basic checks for table:- {table_name}')
    if df.isnull().values.any:
        print(f'null values found in table {table_name}')
        null_found = True
        data[table_name] = fill_null_with_zero(df)
        print(f"filled with null values")

#extract column types
def get_column_datatypes(df):
    return df.dtypes

#Assigning the data types 
def convert_data_types(df, expected_types):
    for col, dtype in expected_types.items():
        if df[col].dtype != dtype:
            if dtype == 'int':
                df[col] = df[col].fillna(0)
            elif dtype == 'float':
                df[col] = df[col].fillna(0.0)
            else:
                df[col] = df[col].fillna('')
            df[col] = df[col].astype(dtype)
            print(f"converted data types {col} to {dtype} in {table_name}")
        else:
            print(f'Error converting {col} to {dtype} in {table_name}')


def aggregate_sales(df):
    group_df = df.groupby(['pos_site_id', 'sku_id', 'fscldt_id', 'price_substate_id','type']).agg({
        'sales_units': "sum",
        'sales_dollars':'sum',
        'discount_dollars':'sum'
    }).reset_index()
    return group_df


expected_types = {
    'order_id': 'int', 'line_id': 'int', 'type': 'str', 'dt':'datetime64[ns]','pos_site_id': 'str','sku_id': 'str', 'fscldt_id':'int',
    'price_substate_id':'str','sales_units':'int', 'sales_dollars':'str','discount_dollars':'str','original_order_id':'int',
    'original_line_id':'int'}

# perform checks for non-unique primary key values in the fact table
for table_name, df in data.items():
    if 'fact' in table_name:
        print('in Fact table Now ----')
        check_non_unique_primary_key(df, table_name)
        # print(df)
        convert_data_types(df, expected_types)
        perform_basic_checks(df, table_name)
        new_df_col = get_column_datatypes(df)
        print(new_df_col)

# Save mview_weekly_sales to CSV file
def save_to_s3(df, bucket_name, file_key):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding= 'utf-8-sig')
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
mview_weekly_sales = aggregate_sales(data['fact'])
print(mview_weekly_sales)

save_to_s3(mview_weekly_sales, bucket_name, 'data/mview_weekly_sales.csv')