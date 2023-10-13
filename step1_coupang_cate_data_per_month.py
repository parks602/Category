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
#     display_name: Python 2
#     language: python
#     name: python2
# ---

# %%
from lens.models import Lens
from django.conf import settings
from IPython.display import display
from common.utils import download_dataframe_to_excel, download_dataframe_to_csv
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from common.utils import download_dataframe_to_csv
from datetime import datetime
from dateutil.relativedelta import relativedelta

mysql = Lens.getConnector(settings.MYSQL_LENS_NAME)
prestoLens = Lens.getConnector(settings.PRESTO_LENS_NAME)


# %%
def get_data(date):
    query="""select distinct(product_id), ep_title, category1, category2, category3, category4  
    from retargeting_event_storage_orc 
    where day = '{date}'
    and retargeting_cd='300' 
    and event_name='product' 
    """.format(date=date)
    df_product = prestoLens.get_dataframe(query)
    return(df_product)


# %%
start_date = datetime.strftime(datetime.today() + relativedelta(months=-1),'%Y%m%d')
end_date   = datetime.strftime(datetime.today() + relativedelta(days=-1),'%Y%m%d')

print(start_date, end_date)

# %%
# start_date = '20230401' 
# end_date = '20230430'
# print(start_date, end_date)

# %%
from tqdm import tqdm 
import os

month_dir = './data/%s'%(datetime.strftime(datetime.today(),'%Y_%m'))

if os.path.exists(month_dir) == False:
    os.mkdir(month_dir)
    
save_dir = '%s/raw'%(month_dir)

if os.path.exists(save_dir) == False:
    os.mkdir(save_dir)
    
f_start_date = datetime.strptime(start_date, '%Y%m%d')
f_end_date = datetime.strptime(end_date, '%Y%m%d')
now = f_start_date
print(now, f_end_date)
date_list = []

while now <= f_end_date:
    str_now = datetime.strftime(now, '%Y%m%d')
    date_list.append(str_now)
    now = now + relativedelta(days=4)   

for i in tqdm(date_list):
    data = get_data(i)
    print(data)
    data['ep_title']=data['ep_title'].str.encode('utf-8')
    data.to_csv('%s/%s.csv'%(str(save_dir), str(i)))
    del data
