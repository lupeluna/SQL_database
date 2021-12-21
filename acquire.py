import pandas as pd
import numpy as np
import os

# acquire
from env import host, user, password
from pydataset import data

# turn off warning boxes
import warnings
warnings.filterwarnings('ignore')


# helper function to get the url connection

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to create a connection url to access SQL database info.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'




def home_credit_dictionary():
    '''
    This function reads the data from the SQL database to df
    '''
    credit_dict = '''SELECT variable_name, description
    FROM data_dictionary
    '''
    
    return pd.read_sql(credit_dict, get_connection('home_credit'))


def customer_applications():
    applications_sql = '''SELECT SK_ID_CURR, TARGET, NAME_CONTRACT_TYPE, CODE_GENDER, CNT_CHILDREN, AMT_INCOME_TOTAL,
    AMT_CREDIT, AMT_ANNUITY, AMT_GOODS_PRICE, NAME_INCOME_TYPE, NAME_EDUCATION_TYPE, NAME_FAMILY_STATUS,
    NAME_HOUSING_TYPE, DAYS_BIRTH
    FROM application_history
    '''
    # JOIN installment_payments using(AMT_PAYMENT)
    
    
    return pd.read_sql(applications_sql, get_connection('home_credit'))


def get_cust_app():
    '''
    This function reads the home_credit data from SQL database and writes data to
    a csv file if, returns df.
    '''
    if os.path.isfile('home_credit_df.csv'):
        # If csv file exists, read in data from csv.
        df = pd.read_csv('home_credit_df.csv', index_col=0)
        
        
    else:
        
        # Read fresh data from db into a DataFrame.
        df = customer_applications()
        # Write DataFrame to a csv file.
        df.to_csv('home_credit_df.csv')
    return df