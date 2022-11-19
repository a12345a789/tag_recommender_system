import pandas as pd
import numpy as np
import collections
import time
from tqdm import tqdm
from threading import Thread
from multiprocessing import Queue
import threading
from queue import Queue
import random
import operator
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
#import saspy
dt=datetime.now().strftime('%Y%m')
###################################################SAS_ETL############################################
#sas=saspy.SASsession(cfgname='iomwin',log4j='2.17.1',omruser='iXXXXX',omrpw='XXXXXX')
#讀取SAS資料館

#ps1=sas.submit('''
#libname abc "D:\SAS_DATA"
#''')

#設定讀檔路徑
os.chdir('/Users/a12345/Desktop/python/專案＿推薦引擎/00_007online_tag_recommendation')
os.getcwd()
input_path='./input_path/'
output_path='./output_path/'

#產出input_path讀檔路徑
try:
    os.makedirs(input_path)
except FileExistsError:
    print('資料夾已存在')
except:
    print('權限不足')

#產出output_path 讀檔路徑
try:
    os.makedirs(output_path)
except FileExistsError:
    print('資料夾已存在')
except PermissionError:
    print('權限不足')

file_path = input_path+"./user_taggedartists.txt"
df_tag=pd.read_csv(file_path,sep='\t')

class tag_EDA(object):
    def __init__(self):
        super().__init__()
    def dataload(self):
        '''
        input : 標籤資料
        
        '''
        print('開始讀取數據...')
        file_path = input_path+"./user_taggedartists.txt"
        df_tag=pd.read_csv(file_path,sep='\t')
        print('標籤庫筆數共{}筆'.format(len(df_tag)))
        
        return df_tag
    def data_processing(self):
        r1=self.dataload()
        tuple_to_list=list(r1)
        df_tag=pd.DataFrame()
    #統計各類標籤數量
    def addValueToMat(self,theMat,key,value,incr):
        if key not in theMat:
            theMat[key]=dict();
        else:
            if value not in theMat[key]:
                theMat[key][value]=incr;
            else:
                theMat[key][value]+=incr
    user_tags=dict();
    tag_items=dict();
    user_items=dict(); #測試集數據字典
    item_tags=dict()   #用於多樣性測試
    
    #初始化，進行各種統計
    def InitStat(self):
        '''
        input1: dict指定的字典
        input2: 指定的鍵值
        input3: tag標籤
        '''
        self.data_processing()
        global user_tags
        global tag_items
        global user_items
        
        user_tags=dict()
        tag_items=dict()
        user_items=dict()
        user_items_test=dict()
        item_tags=dict()
        data_file=open(input_path+'df_all.csv',encoding='utf-8-sig')
        line=data_file.readline();
        while line:
            if random.random()>0.1: #將90%的數據作為訓練集，剩下10%的數據作為測試集
                terms=line.split(",") #訓練集的數據結構是[user,item,tag]形式
                user=terms[0];
                item=terms[1];
                tag=terms[2];
                self.addValueToMat(user_tags,user,tag,1)
                self.addValueToMat(tag_items,tag,item,1)
                self.addValueToMat(user_items,user,item,1)
                self.addValueToMat(item_tags,item,tag,1)
                line=data_file.readline();
            else:
                self.addValueToMat(user_items_test,user,item,1)
        data_file.close()
        
    #統計標籤流行度
    def TagPopularity(self):
        self.InitStat()
        tagfreq={}
        for user in user_tags.keys():
            for tag in user_tags.keys():
                if tag not in tagfreq:
                    tagfreq[tag]=1
                else:
                    tagfreq[tag]+=1
                
        return sorted(tagfreq.items(),key=lambda a:a[1],reverse=True)
    
    #標籤流行度統計
    def tagFreq(self):
        tagFreq=self.TagPopularity()
        for tag in tagFreq[:20]:
            print(tag)
        tag_count= pd.DataFrame(tagFreq,columns=['tag','count'])
        tag_count.to_csv(output_path+'tag_count'+dt+'.csv',index=False,encoding='utf-8-sig')
    
    def run(self):
        self.tagFreq()
def main():
    EDA=tag_EDA()
    EDA.run()
    
if __name__=='__main__':
    main()
        
        
        

