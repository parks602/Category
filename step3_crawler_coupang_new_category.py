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
from selenium import webdriver
from selenium.webdriver.common.by import By

import time
from dateutil.relativedelta import relativedelta

from datetime import datetime
import pandas as pd
from pyvirtualdisplay import Display


# %%
month_dir           = './data/%s'%(datetime.strftime(datetime.today(),'%Y_%m'))
before_month_dir    = './data/%s'%(datetime.strftime(datetime.today() + relativedelta(months=-1),'%Y_%m'))
diff_data           = pd.read_csv('%s/diff_data.csv'%(month_dir), index_col=0)
diff_data['count']  =range(diff_data.shape[0])
print(diff_data.shape)

# %%
li_names = []
for i, cate in enumerate(diff_data['product_id']):
    display = Display(visible=0, size=(800, 600))
    display.start()

    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    #options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
    driver.get('https://www.coupang.com/vp/products/%s?isAddedCart='%(cate))
    time.sleep(2)
    li_name = [i]
    try:
        names = driver.find_elements(by=By.CLASS_NAME, value='breadcrumb-link')
        print(names)
        for name in names[1:]:
            li_name.append(name.get_attribute('title'))
    except:
        print(i, 'UnexpectedAlertPresentException')
        li_name.append(li_name)
        pass
    li_names.append(li_name)
    driver.quit()
    display.stop()

# %%
cate_info = pd.DataFrame(li_names)
if cate_info.shape[1] == 5:
    cate_info['5']=None
cate_info.columns=['count', 'cate1_name', 'cate2_name', 'cate3_name', 'cate4_name', 'cate5_name']

# %%
cate_data = pd.merge(diff_data, cate_info, on='count')

# %%
cate_info

# %%
all_cate = pd.read_csv('%s/coupang_cate_all.csv'%(before_month_dir), index_col=0)

# %%
new_all_cate = pd.concat([all_cate, cate_data])
new_all_cate = new_all_cate.reset_index(drop=True)
new_all_cate.to_csv('%s/coupang_cate_all.csv'%(month_dir))
new_all_cate[['product_id', 'category1', 'category2', 'category3', 'category4']].to_csv('%s/updated_unique.csv'%(month_dir))


# %%
all_cate
