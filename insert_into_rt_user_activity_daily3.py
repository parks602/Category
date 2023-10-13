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


hive_lens = HiveConnector()

# %%
hive_lens = HiveConnector()
query = """
CREATE TABLE IF NOT EXISTS rt_user_activity_daily3 (
  deviceid STRING,
  dow BIGINT,
  product BIGINT,
  cart BIGINT,
  purchase BIGINT,
  revenue DOUBLE,
  avg_pur_rate DOUBLE,
  max_pur_rate DOUBLE,
  avg_price DOUBLE,
  max_price DOUBLE,
  avg_sale DOUBLE,
  max_sale DOUBLE,
  sale_cnt BIGINT,
  pid_map MAP<STRING, BIGINT>,
  cate_map MAP<STRING, BIGINT>,
  seg_map MAP<STRING, BIGINT>,
  brand_map MAP<STRING, BIGINT>,
  hour_map MAP<STRING, BIGINT>,
  cate_map_coupang MAP<STRING, BIGINT>
)
PARTITIONED BY (rt_cd STRING, day STRING, scode_hash STRING)
STORED AS ORC
location 'hdfs://ha-cluster/rt_user_activity_daily3/logs'
"""
hive_lens.runquery(query)

# %%

'''-------------타겟 날짜-------------'''
from datetime import date, timedelta, datetime
set_day=[str((date.today()- timedelta(days=1)).strftime("%Y%m%d"))]

# sday = '20230501'
# sday = datetime.strptime(sday, '%Y%m%d')
# eday = '20230521'
# eday = datetime.strptime(eday, '%Y%m%d')
# set_day =[]
# while sday<=eday:
#     set_day.append(datetime.strftime(sday, '%Y%m%d'))
#     sday += timedelta(days=1)

'''--------------리타게팅 싸이트 목록 가져오기 -------------''' 
# 전체 retargeting cd  (위메프, 직방, 더블유컨셉 제외)
query="""
    select  cast(b.retargeting_cd as varchar) retargeting_cd
    from (
        SELECT adv.advertiser_app_event_cd, external_id
        FROM mysql.cauly.advertiser_app_event_retargeting_info adv
        INNER JOIN mysql.cauly.retargeting r on r.retargeting_cd  = adv.retargeting_cd
        WHERE r.stat = 'start'
        group by advertiser_app_event_cd, external_id
    ) a
    inner join (
        select external_id, retargeting_cd from mysql.cauly.retargeting
        where is_represent = 'Y'
        group by external_id,retargeting_cd
    )
    b 
    on a.external_id = b.external_id
    group by b.retargeting_cd
    """

retargeting_cd_list=presto_lens.get_dataframe(query).retargeting_cd.to_list()
all_rc_list=tuple(set(retargeting_cd_list))

##EP 에서 retargeting cd 필터링
def rt_cd_in_EP(set_day,all_rc_list):
    query =  """
            select retargeting_cd from (
            select retargeting_cd,count() as cnt from retargeting_event_storage_orc where
            day in ('{set_day}')
            and NOT ep_title like ''
            and retargeting_cd in ({all_rc_list})
            group by retargeting_cd)
            where cnt>50
            """.format(set_day=set_day,all_rc_list=",".join([ "'{}'".format(i) for i in list(all_rc_list)]))
    print(query)
    return presto_lens.get_dataframe(query)
#filter_rc_list=rt_cd_in_EP(set_day,all_rc_list).retargeting_cd.to_list()
#print(set_day,filter_rc_list)


# %%
set_day

# %%
scode_hashes=['a','b','c','d','e','f','0','1','2','3','4','5','6','7','8','9']
def write_into_table(retargeting_cd, scode_hash, set_day):
    query="""
    INSERT INTO rt_user_activity_daily3
    with a as(
    select deviceid,hour, event_name, product_id,order_id,cast( revenue as double ) revenue,try_cast(ep_saleprice as double) price, COALESCE(try( ( try_cast(ep_price  as double) - try_cast(ep_saleprice as double))*100/ try_cast(ep_price  as double)),0) sale 
    from retargeting_event_storage_orc where day='{day}' and retargeting_cd='{retargeting_cd}'  and deviceid!='-' and lower(substr(deviceid,4,1))='{scode_hash}'
    ), 
    b as(
    select prod_name , if((p_cnt*100)/(v_cnt+1)>100, 100,(p_cnt*100)/(v_cnt+1) )p_pct,prod_cate,prod_seg,brand from rt_product_daily where day='{day}'  and rt_cd='{retargeting_cd}' and cardinality(prod_cate)>0
    ), 
    c as(
    select prod_name, array_agg(category4) category4 from rt_product_daily_coupang2 where day='{day}'  and rt_cd='{retargeting_cd}' and category4 != '-' group by prod_name
    )
    select a.deviceid,{dow} dow,count( if(a.event_name ='product' , a.product_id)) product,
    count(distinct if(a.event_name ='addToCart' , a.order_id)) cart, 
    count(distinct if(a.event_name ='purchase' , a.order_id)) purchase,
    coalesce(sum(distinct if(a.event_name ='purchase' ,a.revenue)),0 ) revenue,
    avg(b.p_pct) avg_pur_rate,max(b.p_pct) max_pur_rate,
    avg(price)avg_price, max(price) max_price,
    avg(sale) avg_sale, max(sale) max_sale , count( if(sale>0,sale)) sale_cnt,
    TRANSFORM_VALUES(MULTIMAP_FROM_ENTRIES( TRANSFORM(filter(array_agg(a.product_id) , x -> length(x)>1  ), x -> ROW(x, 1)) ), (k, v) -> REDUCE(v, 0, (s, x) -> s + x, s -> s)) 
    pid_map,  
    TRANSFORM_VALUES(MULTIMAP_FROM_ENTRIES( TRANSFORM( transform ( filter( array_agg(prod_cate), x -> cardinality(x)>0 ), x -> element_at(x,1)  ), x -> ROW(x, 1))),(k, v) -> REDUCE(v, 0, (s, x) -> s + x, s -> s) ) 
    cate_map,
    TRANSFORM_VALUES(MULTIMAP_FROM_ENTRIES( TRANSFORM( transform ( filter( array_agg(prod_seg), x -> cardinality(x)>0 ), x -> element_at(x,1)  ), x -> ROW(x, 1))),(k, v) -> REDUCE(v, 0, (s, x) -> s + x, s -> s) ) 
    seg_map,
    TRANSFORM_VALUES(MULTIMAP_FROM_ENTRIES(TRANSFORM(flatten(array_agg(brand)), x -> ROW(x, 1))),(k, v) -> REDUCE(v, 0, (s, x) -> s + x, s -> s)) 
    brand_map,
    TRANSFORM_VALUES(MULTIMAP_FROM_ENTRIES(TRANSFORM(array_agg(hour), x -> ROW(x, 1))),(k, v) -> REDUCE(v, 0, (s, x) -> s + x, s -> s)) 
    hour_map,
    TRANSFORM_VALUES(MULTIMAP_FROM_ENTRIES( TRANSFORM( transform ( filter( array_agg(category4), x -> cardinality(x)>0 ), x -> element_at(x,1)  ), x -> ROW(x, 1))),(k, v) -> REDUCE(v, 0, (s, x) -> s + x, s -> s) ) 
    cate_map_coupang,'{retargeting_cd}' rt_cd  ,'{day}' day, '{scode_hash}' scode_hash
    from a 
    left join b on a.product_id=b.prod_name 
    left join c on a.product_id=c.prod_name 
    group by deviceid 
    """.format(retargeting_cd = retargeting_cd ,day=set_day, dow=str(pd.to_datetime(set_day).strftime('%w')),scode_hash=scode_hash)
    d = presto_lens.get_dataframe(query)

import time 
prev_t = time.time()
for target_day in set_day:
    print('target',target_day)
    filter_rc_list=rt_cd_in_EP(target_day,all_rc_list).retargeting_cd.to_list()

    for retargeting_cd in filter_rc_list:
        print(type(retargeting_cd), target_day)
        for scode_hash in scode_hashes:
            write_into_table(retargeting_cd,scode_hash, target_day )
        print('=='*20, time.time()-prev_t)

# %%
import requests, json

api_url = 'http://sms.fsnsys.com:10083/sms/sms_process.php'
headers = {
    "Content-type": "application/json"
}
data = json.dumps({
    "rcv":"alert-tf_idf, 01033026840",
    "subject":"insert_into_rt_user_activity_daily3",
    "content":"쿠팡 기반 카테고리 생성 이후 rt_user_activity_daily3 테이블 생성 종료"
})

response = requests.post(api_url, headers=headers, data=data)
print(response)

# %%
# a = presto_lens.get_dataframe("""select * from rt_user_activity_daily3 where day = '20230507' and rt_cd = '347'""")
# b = presto_lens.get_dataframe("""select * from rt_user_activity_daily2 where day = '20230507' and rt_cd = '347'""")

# %%
# from IPython.core.display import HTML
# HTML("""
#     <style>
#     .container {
#         width:95% !important;
#     }
#     </style>
# """)

# %%
# query = """
# DROP TABLE rt_user_activity_daily3"""
# hive_lens.runquery(query)

# %%
# a[a['deviceid']=='ae51ae00-9b03-47bd-84ab-f73d908398ad']

# %%
# a = presto_lens.get_dataframe("""select * from rt_product_daily_coupang2 where day = '20230420' limit 10""")
# a
