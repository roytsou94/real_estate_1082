#!/usr/bin/env python
# coding: utf-8

# 1.撰寫網路爬蟲程式抓取資料

import requests,os,zipfile,time

def real_estate_crawler(year, season):
  if year > 1000:
    year -= 1911

  res = requests.get("https://plvr.land.moi.gov.tw//DownloadSeason?season="+str(year)+"S"+str(season)+"&type=zip&fileName=lvr_landcsv.zip")

  fname = str(year)+str(season)+'.zip'
  open(fname, 'wb').write(res.content)

  folder = 'real_estate' + str(year) + str(season)
  if not os.path.isdir(folder):
    os.mkdir(folder)

  with zipfile.ZipFile(fname, 'r') as zip_ref:
    zip_ref.extractall(folder)

  time.sleep(10)

real_estate_crawler(108,2)

# 2.使用Spark讀取資料集

from pyspark.sql import SparkSession,SQLContext, Row
from pyspark import SparkContext

sc = SparkContext()
spark = SQLContext(sc)

a = spark.read.csv('real_estate1082/a_lvr_land_a.csv',header=True)
b = spark.read.csv('real_estate1082/b_lvr_land_a.csv',header=True)
e = spark.read.csv('real_estate1082/e_lvr_land_a.csv',header=True)
f = spark.read.csv('real_estate1082/f_lvr_land_a.csv',header=True)
h = spark.read.csv('real_estate1082/h_lvr_land_a.csv',header=True)

# 3.使用Spark合併資料集

df = a.union(b).union(e).union(f).union(h)
df.show(5)

# 3.1篩選「主要用途」為「住家用」

df_1 = df.filter(df["主要用途"]=="住家用")

# 3.2篩選「建物型態」為「住宅大樓」

df_2 = df_1.filter(df_1.建物型態.contains('住宅大樓'))

# 3.3篩選「總樓層數」需「大於等於十三層」

df_3 = df.filter((df_2["總樓層數"]!="一層")&(df_2["總樓層數"]!="二層")&
                 (df_2["總樓層數"]!="三層")&(df_2["總樓層數"]!="四層")&
                 (df_2["總樓層數"]!="五層")&(df_2["總樓層數"]!="六層")&
                 (df_2["總樓層數"]!="七層")&(df_2["總樓層數"]!="八層")&
                 (df_2["總樓層數"]!="九層")&(df_2["總樓層數"]!="十層")&
                 (df_2["總樓層數"]!="十一層")&(df_2["總樓層數"]!="十二層"))

df_3.show(5)

# 4.使用Spark將篩選結果轉換成json格式，產生「result-part1.json」和「result-part2.json」(五個城市隨機放入兩個檔案)

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

df_4 = df_3.toPandas()
type(df_4)

# 隨機放入兩個資料集

part1 = df_4.sample(frac=0.5, random_state=1)
part2 = df_4.drop(part1.index)

print('各資料集大小：', 
      len(df_4), len(part1), len(part2))

# 根據date作desc排序

result_part1 = part1.sort_values(by='交易年月日', ascending=False).drop_duplicates()
result_part2 = part2.sort_values(by='交易年月日', ascending=False).drop_duplicates()
display(result_part1.head())
display(result_part2.head())

# 產生json檔案

result_part1.to_json(r'result-part1.json')
result_part2.to_json(r'result-part2.json')

# End

