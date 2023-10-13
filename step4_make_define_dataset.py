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
import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm

# %%
month_dir    = './data/%s'%(datetime.strftime(datetime.today(),'%Y_%m'))
raw_data_dir = '%s/raw'%(month_dir)
# ldir         = os.listdir('%s'%(raw_data_dir))

# %%
cate_data = pd.read_csv('%s/coupang_cate_all.csv'%(month_dir),index_col=0)
cate_data = cate_data.dropna(subset =['category4'], axis=0)

# %%
cate_data[cate_data['category4'].isna()]

# %%
k  = cate_data.dropna(subset=['cate1_name']).drop_duplicates(['cate4_name'])
k = k.reset_index(drop=True)
k

# %%
p  = cate_data.dropna(subset=['cate1_name'])
p = p.reset_index(drop=True)
p

# %%
nul_data = cate_data[cate_data['cate1_name'].isnull()]
nul_data = nul_data[['category4', 'cate1_name']]
nul_data.rename(columns = {'cate1_name' : 'new_cate'}, inplace = True)
nul_data['new_cate'] = 1.0
nul_data = nul_data.reset_index(drop = True)
nul_data

# %%
import numpy as np
new_cate = pd.DataFrame(columns = ('category4', 'new_cate'))
for i in range(p.shape[0]):
    if (k['category4']==p.category4[i]).any():
        data = [p.category4[i], p.category4[i]]
    elif len(k[p.cate4_name[i] == k.cate4_name].category4.values) ==0:
        data = [p.category4[i], k[p.cate1_name[i] == k.cate1_name].category4.values[0]]
    else:
        data = [p.category4[i], k[p.cate4_name[i] == k.cate4_name].category4.values[0]]
    new_cate.loc[len(new_cate)] = data

# %%
new_cate = pd.concat([new_cate, nul_data]).reset_index(drop=True)

# %%
new_cate[['category4', 'new_cate']] = new_cate[['category4', 'new_cate']].astype('int')

# %%
new_cate

# %%
# replace_dict = new_cate.set_index('category4')['new_cate'].to_dict()
# replace_dict = {str(k): str(v) for k, v in replace_dict.items()}

# %%
my_dict = dict(zip(new_cate['category4'].values.tolist(), new_cate['new_cate'].values.tolist()))

# %%
p[p['category4'].isin(new_cate['new_cate'])].reset_index(drop=True).to_csv('%s/cate_dictionary.csv'%(month_dir))

# %%
len(new_cate['category4'].unique())

# %%
len(new_cate['new_cate'].unique())

# %%
save_dir = '%s/define'%(month_dir)
if os.path.exists(save_dir) == False:
    os.mkdir(save_dir)
for n in tqdm(ldir): 
    print('%s/%s'%(raw_data_dir, n))
    m = pd.read_csv('%s/%s'%(raw_data_dir, n), index_col=0)
    m = m[['ep_title', 'category4']]
    m = m.dropna(subset =['category4'], axis=0).reset_index(drop=True)
    m['category4'] = m['category4'].astype(int)
    m['category4'].replace(to_replace=my_dict, inplace = True) 
    print(m.shape)
    m = m.reset_index(drop = True)
    m.to_csv('%s/%s'%(save_dir, n))
    print(n)

