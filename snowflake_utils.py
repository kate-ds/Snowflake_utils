import snowflake.connector
import pandas as pd
from tqdm import tqdm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
import os


class SnowflakeDataConnector:
    """
    A utility class for interacting with Snowflake databases.
    ---------------------------------------------------------

    Args:
        user (str):                    Snowflake username.
        authenticator (str, optional): Snowflake authenticator type. Defaults to 'externalbrowser'.
        account (str, optional):       Snowflake account name. Defaults to 'prod'.
        tech_login (str, optional):    Snowflake technical user login.
        tech_key (str, optional):      Snowflake technical user private key.

    Attributes:
        connector: Snowflake connection object.

    Methods:
        connect_user:            Connect to Snowflake using user credentials.
        connect_tech:            Connect to Snowflake using technical account.
        create_temp_tbl:         Create a temporary table in Snowflake.
        download_data:           Download data from Snowflake and save it to pickle files.
        execute_query_to_pandas: Execute a SQL query and return the result as a DataFrame.
        upload_data:             Upload data from a DataFrame to Snowflake.
        delete_data:             Delete data from table in Snowflake.
        disconnect:              Disconnect from Snowflake.
    """

    
    def __init__(self, authenticator='externalbrowser', account='prod', user=None, tech_login=None, tech_key=None):
        self.user = user
        self.tech_login = tech_login
        self.tech_key = tech_key
        self.authenticator = authenticator
        self.account = account
        self.connector = None
        if self.user:
            self.connect_user()
        elif self.tech_login and self.tech_key:
            self.connect_tech()
        else:
            self.user = input("snowflake user: ")

            
    def connect_user(self):
        """
        Connect to Snowflake using user credentials.
        """
        try:
            self.connector = snowflake.connector.connect(
                user=self.user,
                authenticator=self.authenticator,
                account=self.account
            )
            print(f"User {self.user} Connected to Snowflake")
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")

            
    def connect_tech(self):
        """
        Connect to Snowflake using technical account.
        """
        try:
            # replace \n back to newline
            private_key_str = self.tech_key.replace('\\n', '\n')
            # load the private key
            private_key = serialization.load_pem_private_key(
                data=private_key_str.encode(),
                password=None,
                backend=default_backend()
            )
            # serialize the private key into DER format
            private_key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            # establish the connection
            self.connector = snowflake.connector.connect(
                user=self.tech_login,
                account=self.account,
                private_key=private_key_der
            )
            print(f"Connected to Snowflake with Tech acccount")
        except Exception as e:
            print(f"Error creating temporary table: {str(e)}")

            
    def create_temp_tbl(self, database, schema, tmp_table_name, tmp_query):
        """
        Create a temporary table in Snowflake.
        --------------------------------------

        Args:
            database (str):       Snowflake database name.
            schema (str):         Snowflake schema name.
            tmp_table_name (str): Name of the temporary table to create.
            tmp_query (str):      SQL query to populate the temporary table.
        """
        try:
            cursor = self.connector.cursor()
            cursor.execute(
                f"""CREATE OR REPLACE TEMPORARY TABLE {database}.{schema}.{tmp_table_name} AS ({tmp_query});""")
            cursor.close()
        except Exception as e:
            print(f"Error creating temporary table: {str(e)}")
        finally:
            cursor.close()

            
    def download_data(self,
                      query,
                      depth=10,
                      batch=1000000,
                      join=True,
                      raw_folder_path='data/raw',
                      full_file_folder='data/',
                      file_name="data"):
        """
        Download data from Snowflake and save it to pickle files.
        ---------------------------------------------------------

        Args:
            query (str):                      SQL query to fetch data from Snowflake.
            depth (int, optional):            Number of iterations to fetch data in batches. Defaults to 10.
            batch (int, optional):            Batch size for fetching data. Defaults to 1000000.
            join (bool, optional):            Whether to join the downloaded files into a single file. Defaults to True.
            raw_folder_path (str, optional):  Path to store raw pickle files. Defaults to 'data/raw'.
            full_file_folder (str, optional): Folder path for the joined pickle file. Defaults to 'data/'.
            file_name (str, optional):        Base name for saved files. Defaults to "data".
        """
        for path in [raw_folder_path, full_file_folder]:
            if not os.path.exists(path):
                os.makedirs(path)
        try:
            print(f"{file_name} data loading...")
            cursor = self.connector.cursor()
            cursor.execute(query)
            for cur_set in tqdm(range(depth)):
                data = cursor.fetchmany(batch)
                if not data:
                    break
                df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
                df.to_pickle(f'{raw_folder_path}/{file_name}__p_{cur_set}.pkl')
            print(f"{file_name} data loaded in {raw_folder_path}, {len(os.listdir(raw_folder_path))} files")
            # Join files
            if join:
                file_path = f'{full_file_folder}/{file_name}.pkl'
                df = []
                for cur_set in tqdm(range(depth)):
                    file = f'{raw_folder_path}/{file_name}__p_{cur_set}.pkl'
                    if os.path.exists(file):
                        df.append(pd.read_pickle(file))
                df = pd.concat(df)
                df.to_pickle(file_path)
            cursor.close()
        except Exception as e:
            print(f"Error downloading data from query: {str(e)}")
        finally:
            cursor.close()

            
    def execute_query_to_pandas(self, query):
        """
        Execute a SQL query to pandas DataFrame.
        ----------------------------------------

        Args:
            query (str): SQL query to execute.

        Returns:
            pandas.DataFrame: Resulting DataFrame.
        """
        try:
            cursor = self.connector.cursor()
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            return df
        except Exception as e:
            print(f"Error executing query: {str(e)}")
        finally:
            cursor.close()

            
    def upload_data(self, df, database, schema, table_name, if_exists="append"):
        """
        Upload data from a DataFrame to Snowflake.
        ------------------------------------------

        Args:
            df (pandas.DataFrame):     DataFrame containing data to upload.
            database (str):            Snowflake database name.
            schema (str):              Snowflake schema name.
            table_name (str):          Name of the table to upload data into.
            if_exists (str, optional): Action to take when the table already exists. Defaults to "append"
                Supported values:
                - "fail":              Raise an error if the table already exists.
                - "replace":           Replace the existing data with the new data.
                - "append":            Append the data to the existing table.
                - "drop_table"         Drop table if the data already exists.
        """
        df.columns = [col.upper() for col in df.columns]

        try:
            cursor = self.connector.cursor()
            cursor.execute(f'USE DATABASE {database}')
            cursor.execute(f'USE SCHEMA {schema}')
            print("Uploading....")
            if if_exists=='drop_table':
                print('Drop table...')
                cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                if_exists = 'append'

            # Create SQLAlchemy engine using the Snowflake connection
            engine = create_engine(
                "snowflake://",
                creator=lambda: self.connector,
                connect_args={"connector": self.connector},
                pool_pre_ping=True
            )
            df.to_sql(table_name, engine, index=False, method=pd_writer, if_exists=if_exists)
            cursor.close()
            return f"Data uploaded to {database}.{schema}.{table_name} - {len(df)} rows"
        except Exception as e:
            print(f"Error uploading data to snowflake: {str(e)}")
        finally:
            cursor.close()
            
            
    def delete_data(self, database, schema, table_name, drop_condition='', drop_all=False):
        """
        Delete data from table in Snowflake.
        --------------------------------------

        Args:
            database (str):            Snowflake database name.
            schema (str):              Snowflake schema name.
            table_name (str):          Name of the table.
            drop_condition (str):      Condition after 'where' (example: PREDICTION_AT='1990-05-15')
            drop_all (bool, optional): Arg if you want to clean table (Default - False)
        """
        try:
            cursor = self.connector.cursor()
            cursor.execute(f'USE DATABASE {database}')
            cursor.execute(f'USE SCHEMA {schema}')

            if not drop_all:
                if not drop_condition:
                    drop_condition = input("condition after WHERE: ")
                print(f"delete part - {drop_condition}")
                cursor.execute(f"""DELETE FROM {table_name}
                                    WHERE {drop_condition}
                                    """)
                print("done")
                cursor.close()

            else:
                check = input(f"Are you sure you want to clean table {database}.{schema}.{table_name}? (yes/no)")
                if check=='yes':
                    cursor.execute(f"USE DATABASE {database}")
                    cursor.execute(f"USE SCHEMA {schema}")
                    print(f"delete all")
                    cursor.execute(f"""DELETE FROM {table_name}""")
                    print("done")
                    cursor.close()
                else:
                    return None                
        except Exception as e:
            print(f"Error: {str(e)}")
        finally:
            cursor.close()

            
    def disconnect(self):
        """
        Close Snowflake connection.
        """
        if self.connector:
            self.connector.close()
            print("Disconnected from Snowflake.")
            