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
import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from tqdm import tqdm 

# %%
month_dir        = './data/%s'%(datetime.strftime(datetime.today(),'%Y_%m'))
before_month_dir = './data/%s'%(datetime.strftime(datetime.today() + relativedelta(months=-1),'%Y_%m'))
load_dir         = '%s/raw'%(month_dir)
load_list        = os.listdir(load_dir)
load_list = list(map(str, load_list))

# %%
load_list

# %%
for j, i in tqdm(enumerate(load_list)):
    data = pd.read_csv('%s/%s'%(load_dir, i), index_col=0).drop_duplicates(subset=['category1', 'category2', 'category3', 'category4'])
    if j == 0:
        dataset = data
    else:
        dataset = pd.concat([dataset, data]).drop_duplicates(subset=['category1', 'category2', 'category3', 'category4'])
    del data
        
dataset = dataset.reset_index(drop=True)


# %%
len(dataset)

# %%
dataset.head()

# %%
dataset = dataset.dropna(subset=(['category4']))

# %%
len(dataset)

# %%
dataset.to_csv('%s/new_unique.csv'%(month_dir))
new = dataset['category4'].unique().tolist()

# %%
original = pd.read_csv('%s/updated_unique.csv'%(before_month_dir), index_col=0)
o = original['category4'].unique().tolist()

# %%
diff = [x for x in new if x not in o]

# %%
diff

# %%
# print(len(new))
# print(len(o))
# print(len(diff))

# %%
diff_data = dataset[dataset['category4'].isin(diff)].reset_index(drop=True)
diff_data.to_csv('%s/diff_data.csv'%(month_dir))

# %%
diff_data

# %%
