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
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# %%
train_month  = datetime.strftime(datetime.today(),'%Y_%m')
month_dir    = './data/%s'%(datetime.strftime(datetime.today(),'%Y_%m'))
define_dir   = '%s/define'%(month_dir)
datalist = os.listdir(define_dir)

select_num = 3000

cate_dict = pd.read_csv('%s/fianl_cate_dictionary.csv'%(month_dir), index_col=0)

# %%
for num, data_name in enumerate(tqdm(datalist)):
    print(data_name)
    datas = pd.read_csv('%s/%s'%(define_dir, data_name), index_col=0)
    datas = datas[['ep_title','category4']].dropna(axis=0)
    datas = datas.reset_index(drop=True)
    if num == 0:
        df = datas
    else:
        df = pd.concat([df, datas])
    del datas
    
df = df.drop(df[df['category4']==1].index)
df = df.groupby('category4').head(select_num)
df = df.reset_index(drop=True)
df.to_csv('%s/coupang_train_%s_%s.csv'%(month_dir, select_num, train_month))

# %%
df.columns = ['ep_title', 'old_cate']

# %%
df = df.astype({'old_cate':'int'})

# %%
merged_df = pd.merge(df, cate_dict, left_on='old_cate', right_on='lv4_code')

# %%
df = merged_df[['ep_title', 'new_lv4_code']]

# %%
df.columns = ['ep_title', 'category4']

# %%
len(df['category4'].unique())

# %%

import re
df['product_word']=df['ep_title'].str.replace('[^가-힣]', ' ')
df['product_word']=df['product_word'].str.strip()
df_new = df[['product_word','category4']]
df_train = df_new

# %%
train_arr = [ [j for j in i.split(' ') if len(j)>0] for i in df_train.product_word.values ] 

# %%
cate4_counts_map=df_train.category4.value_counts().to_dict()

# %%
from itertools import combinations

btm_dict = {}
def getkey(a,b):
    if (a,b) in btm_dict:
        return (a,b)
    else:
        return (b,a)
def put(a,b, value):
    key = getkey(a,b)
    if key not in btm_dict :
        btm_dict[key] = {value:1}
    else:
        if value in btm_dict[key]:
            btm_dict[key][value] += 1
        else:
            btm_dict[key][value] = 1


# %%

import time
t = time.time()
for item, keyword in zip(df_train.product_word.values, df_train.category4.values):
    try:
        for a in [i for i in item.split(' ') if len(i)>1]:
            put(a,'',keyword)
        for a,b in (combinations( [i for i in item.split(' ') if len(i)>1] ,2)):
            put(a,b,keyword)
    except Exception as e:
#         print(item)
        pass

time.time()-t


# %%
from collections import Counter
# Counter(btm_dict).most_common(10)
import math
import numpy as np
def softmax_func( x_data):
    predictions = x_data - (x_data.max(axis=1).reshape([-1, 1]))
    softmax = np.exp(predictions)
    softmax /= softmax.sum(axis=1).reshape([-1, 1])
    return softmax
btm_percent_dict = {}
for key in btm_dict:
    val = []
    for i in btm_dict[key]:
        val.append( btm_dict[key][i]*40/cate4_counts_map[i])
    btm_percent_dict[key]={}
    for i,b in zip(list(btm_dict[key].keys()),softmax_func(np.array([val]))[0] ): 
        btm_percent_dict[key][i] = b
    


# %%
import pickle

btm_percent_dict2 = {}
for key in btm_percent_dict:
    btm_percent_dict2[key]={}
    for c in btm_percent_dict[key]:
        if btm_percent_dict[key][c]>0.05:
            btm_percent_dict2[key][c] = btm_percent_dict[key][c]
with open('%s/btm_percent_dict_%s.pickle'%(month_dir, train_month),'wb') as fw:
    pickle.dump(btm_percent_dict2, fw)
