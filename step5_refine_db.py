# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
print('hello')
import pandas as pd
import numpy as np
import os
from pyhive import presto, hive
import time
import pickle
from datetime import datetime, timedelta
import multiprocessing as mp
import warnings
from dateutil.relativedelta import relativedelta

warnings.filterwarnings(action='ignore')
class PrestoConnector(object):
    def __init__(self, param, source='jupyter'):
        self.param = param
        self.connect(source=source)
        
    def connect(self, source='jupyter'):
        param = self.param
        self.conn = presto.connect(host=param['host'], port=param['port'], source=source)

    def runquery(self, query):
        try:
            query = query.strip()
            if query[-1] == ';':
                query = query[:-1]
            cursor = self.conn.cursor()
            cursor.execute(query)
        except (AttributeError, pyhive.exc.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)
        return cursor

    def get_dataframe(self, query, verbose=False):
        cursor = self.runquery(query)
        data = cursor.fetchall()
        if len(data) == 0:
            print ('row is 0')
            return
        df = pd.DataFrame(list(data))
        df.columns = [ d[0] for d in cursor.description ]
        return df
    
presto_lens =PrestoConnector({'host':'cauly161.fsnsys.com','port':8080})

import pyhive
class HiveConnector:
    def __init__(self):
        '''
        Constructor
        '''
        self.param = {"username": "cauly", "host": "hmn.fsnsys.com", "port": 9100, "auth": "NOSASL"}
        self.connect()

    def connect(self):
        param = self.param
        self.conn = hive.connect(host=param['host'], port=param['port'], username=param['username'],
                                auth=param.get('auth', 'NONE'),
                                database=param.get('database', 'default'))

    def runquery(self, query):
        """
        result = self.cursor.execute(query)
        """
        try:
            if query[:-1] == ';':
                query = query[:-1]
            cursor = self.conn.cursor()
            cursor.execute(query)
        #except (AttributeError, hive.OperationalError):
        except (AttributeError, pyhive.exc.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)

        return cursor
    
    def get_dataframe(self, query, verbose=False):
        cursor = self.runquery(query)
        data = cursor.fetchall()
        if len(data) == 0:
            print ('row is 0')
            return
        df = pd.DataFrame(list(data))
        df.columns = [ d[0] for d in cursor.description ]
        return df

    def add_partition(self, table_name, partition):
        """
        partition = {partition_column:partition_col_value, ...}
                  = 'partition_column1=partition_col_value1/partition_column2=partition_col_value2/...'
        partition_spec = (partition_column = partition_col_value, ...)
        """
        if type(partition)==dict:
            partition_spec = ','.join(["%s = '%s'"%(partition_column, partition_col_value) for partition_column, partition_col_value in partition.items()])
        elif type(partition)==str or type(partition)==unicode:
            partition_spec = ','.join(["%s = '%s'"%(p.split('=')[0], p.split('=')[1]) for p in partition.split('/')])
        self.runquery("ALTER TABLE {table} ADD IF NOT EXISTS PARTITION ({partition_spec})".format(table=table_name, partition_spec=partition_spec))


hive = HiveConnector()

# %%
month_dir       = './data/%s'%(datetime.strftime(datetime.today(),'%Y_%m'))
cate_table_name = 'rt_category_info_coupang'
cate_date       = datetime.strftime(datetime.today(),'%Y%m')+'01'
before_date     = datetime.strftime(datetime.strptime(cate_date, '%Y%m%d') - relativedelta(months=1), '%Y%m%d')
before_date

# %%
a  = pd.read_csv('%s/cate_dictionary.csv'%(month_dir), index_col = 0)
a.drop(['product_id','count','cate5_name','ep_title'], axis=1, inplace=True)
a.columns = ['lv1_code','lv2_code','lv3_code','lv4_code','lv1_name','lv2_name','lv3_name','lv4_name']
a.drop_duplicates(subset = (['lv1_name','lv2_name','lv3_name','lv4_name']), inplace=True)
a = a.astype({'lv1_code':'int', 'lv2_code':'int','lv3_code':'int','lv4_code':'int','lv1_name':'string','lv2_name':'string','lv3_name':'string','lv4_name':'string'}).reset_index(drop=True)

# %%
a.shape

# %%
query = """
select *
from {cate_table_name}
where day = '{before_date}'
""".format(cate_table_name=cate_table_name, before_date =before_date)
before_data = presto_lens.get_dataframe(query)
before_data.head()

# %%
lv1_num = len(before_data['new_lv1_code'].unique())+1
lv2_num = len(before_data['new_lv2_code'].unique())+1
lv3_num = len(before_data['new_lv3_code'].unique())+1
lv4_num = len(before_data['new_lv4_code'].unique())+1

# %%
result = pd.merge(a, before_data, on=['lv1_name','lv2_name','lv3_name','lv4_name'], how='left')
result = result.rename(columns={'lv1_code_x': 'lv1_code', 'lv2_code_x':'lv2_code', 'lv3_code_x':'lv3_code', 'lv4_code_x':'lv4_code'})
result = result[['lv1_code','lv2_code','lv3_code','lv4_code','lv1_name','lv2_name','lv3_name','lv4_name', 'new_lv1_code', 'new_lv2_code', 'new_lv3_code', 'new_lv4_code']]
result.head()

# %%
null_row = result[result[['new_lv1_code', 'new_lv2_code', 'new_lv3_code', 'new_lv4_code']].isnull().any(axis=1)]
null_row

# %%
print(lv1_num, lv2_num, lv3_num,lv4_num)

# %%
for i in null_row.index:
    try:
        arr = result.loc[(result['lv1_name'] == result.loc[i]['lv1_name']) & (result['new_lv1_code'] != np.nan)]['new_lv1_code'].values.astype(float)
        result['new_lv1_code'][i] = str(int(arr[~np.isnan(arr)][0])).zfill(4)
    except:
        result['new_lv1_code'][i] = str(lv1_num).zfill(4)
        lv1_num += 1
    try:
        arr = result.loc[(result['lv2_name'] == result.loc[i]['lv2_name']) & (result['new_lv2_code'] != np.nan)]['new_lv2_code'].values.astype(float)
        result['new_lv2_code'][i] = str(int(arr[~np.isnan(arr)][0])).zfill(4)
    except:
        result['new_lv2_code'][i] = str(lv2_num).zfill(4)
        lv2_num += 1
    try:
        arr = result.loc[(result['lv3_name'] == result.loc[i]['lv3_name']) & (result['new_lv3_code'] != np.nan)]['new_lv3_code'].values.astype(float)
        result['new_lv3_code'][i] = str(int(arr[~np.isnan(arr)][0])).zfill(4)
    except:
        result['new_lv3_code'][i] = str(lv3_num).zfill(4)
        lv3_num += 1
    try:
        arr = result.loc[(result['lv4_name'] == result.loc[i]['lv4_name']) & (result['new_lv4_code'] != np.nan)]['new_lv4_code'].values.astype(float)
        result['new_lv4_code'][i] = str(int(arr[~np.isnan(arr)][0])).zfill(4)
    except:
        result['new_lv4_code'][i] = str(lv4_num).zfill(4)
        lv4_num += 1
print(lv1_num, lv2_num, lv3_num,lv4_num)

# %%
result['day']=cate_date

# %%
results=result.astype({'lv1_code':'int', 'lv2_code':'int','lv3_code':'int','lv4_code':'int','lv1_name':'string','lv2_name':'string','lv3_name':'string','lv4_name':'string','new_lv1_code':'string','new_lv2_code':'string','new_lv3_code':'string','new_lv4_code':'string','day':'string'})

# %%
results['new_lv1_code'][0]

# %%
results.to_csv('%s/fianl_cate_dictionary.csv'%(month_dir))

# %%
result

# %%
cate_table_name = 'rt_category_info_coupang'
cate_date = datetime.strftime(datetime.today(),'%Y%m')+'01'


# %%
def create_cate_info_table(table_name):
    create_table_query = """
    CREATE table IF NOT EXISTS rt_category_info_coupang (
    lv1_code int,
    lv2_code int,
    lv3_code int,
    lv4_code int,
    lv1_name string,
    lv2_name string,
    lv3_name string,
    lv4_name string,
    new_lv1_code string,
    new_lv2_code string,
    new_lv3_code string,
    new_lv4_code string
    )
    
    PARTITIONED BY (day string)
    STORED AS PARQUET
    LOCATION 'hdfs://ha-cluster/rt_category_info_coupang/logs'
    """
    print(create_table_query)
    hive.runquery(create_table_query)


# %%
create_cate_info_table(cate_table_name)

# %%
from hdfs import InsecureClient
result.to_parquet('cate_info.parquet')
with open('cate_info.parquet','rb') as reader:
    PROD_OUTPUT_DF = reader.read()

# %%
from hdfs import InsecureClient
def get_url(hosts, port):
    url = ''

    if type(hosts) == list:
        url = ';'.join(['http://' + host+':'+ str(port) for host in hosts])
    else:
        url = 'http://' + hosts + ':' + str(port)
    return url

def save_2_hdfs(df_result, set_day):

    CONF_WEBHDSF = {"host": ["cauly161.fsnsys.com", "cauly162.fsnsys.com"], "port": 50070, "user": "cauly"}
    url = get_url(CONF_WEBHDSF['host'],CONF_WEBHDSF['port'])
    print(url)
    user = CONF_WEBHDSF['user']

    client = InsecureClient(url, user=user)

    with client.write('/rt_category_info_coupang/logs/day={set_day}/result.parquet'.format(set_day=set_day), overwrite=True) as writer:
        writer.write(df_result)

save_2_hdfs(PROD_OUTPUT_DF, cate_date)

# %%
from pyhive import hive
class HiveConnector:
    def __init__(self):
        '''
        Constructor
        '''
        self.param = {"username": "cauly", "host": "hmn.fsnsys.com", "port": 9100, "auth": "NOSASL"}
        self.connect()

    def connect(self):
        param = self.param
        self.conn = hive.connect(host=param['host'], port=param['port'], username=param['username'],
                                auth=param.get('auth', 'NONE'),
                                database=param.get('database', 'default'))

    def runquery(self, query):
        """
        result = self.cursor.execute(query)
        """
        try:
            if query[:-1] == ';':
                query = query[:-1]
            cursor = self.conn.cursor()
            cursor.execute(query)
        except (AttributeError, hive.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)

        return cursor

    def add_partition(self, table_name, partition):
        """
        partition = {partition_column:partition_col_value, ...}
                  = 'partition_column1=partition_col_value1/partition_column2=partition_col_value2/...'
        partition_spec = (partition_column = partition_col_value, ...)
        """
        if type(partition)==dict:
            partition_spec = ','.join(["%s = '%s'"%(partition_column, partition_col_value) for partition_column, partition_col_value in partition.items()])
        elif type(partition)==str or type(partition)==unicode:
            partition_spec = ','.join(["%s = '%s'"%(p.split('=')[0], p.split('=')[1]) for p in partition.split('/')])
        print(partition_spec)
        self.runquery("ALTER TABLE {table} ADD IF NOT EXISTS PARTITION ({partition_spec})".format(table=table_name, partition_spec=partition_spec))


hive = HiveConnector()
hive.add_partition("rt_category_info_coupang", {"day": cate_date})
