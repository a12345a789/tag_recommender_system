#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 10:34:10 2022

@author: a12345
"""

import time
Que = Queue() #初始化隊伍
threads = []

#定義一個多線程執行緒
class myThread(threading.Thread):
    def __init__(self, name, link_range):
        threading.Thread.__init__(self)
        self.name = name
        self.df=df
        self.link_range = link_range
    def run(self):
        print("Starting " + self.name)
        #tf_IDF(self.name,df, self.link_range)
        print("Exiting " + self.name)

#定義模型執行緒
class abt_table(object):
    def __init__(self):
        super().__init__()
    def get_data(self):
        df=pd.read_csv(input_path + "./user_taggedartists.txt",sep='\t')
        df.to_csv(output_path+ 'df.csv',encoding='utf-8-sig')
        return df
    def tf_IDF(self,df):
    
        '''
        :param df：用户對藝術家的打標籤數據集
        return 返回用户與標籤的TF-IDF值。用户：{標籤1：依賴度，標籤2：依賴度。。。}

        '''
        self.get_data()
        urls= list(set(df['userID']))
        link_range_list =[(0,300),(301,600),(601,900),(901,1200),(1201,1700)]
        TF_IDF = dict() #存儲用戶對標籤的依賴度，TF_IDF(u,t) = TF(u,t)*IDF(u,t)，用户：{標籤1：依賴度，標籤2：依賴度。。。}
        N = df.shape[0] # 所有用戶對所有標籤的標記次數和，IDF的分子部分
        tags_times = df['tagID'].value_counts() #所有用戶掉標籤的標記合計
        IDF = dict(zip(tags_times.index,np.log(N/(tags_times+1)))) #標籤：IDF值

        user_tags = dict() #儲存用户ID,以及用户打標籤和打標籤次数
        TF = dict() #計算用户u對標籤t的TF值，用户：{標籤1：TF值，標籤2：TF值。。。}
        user_li = list(set(df['userID']))

        for user in tqdm(user_li[0:1706]): #暫時指定1706的資料，避免出現1707list_index_out_of_range的問題
            Que.put(user_li[user])
            tag_times = df[df['userID'] == user]['tagID'].value_counts() #用户user對打標籤的次數
            di = dict(zip(tag_times.index,tag_times))
            user_tags.setdefault(user,di)

            tag_fre = tag_times/sum(tag_times) #用户user對每个標籤的TF值
            di = dict(zip(tag_fre.index,tag_fre))
            TF.setdefault(user,di) 

            user_tfidf = dict()
            for tag in tag_times.index:
                user_tfidf[tag] = TF[user][tag] * IDF[tag]

            TF_IDF.setdefault(user,user_tfidf)

        return TF_IDF
    
    def time_calculate(self):
        link_range_list =[(0,300),(301,600),(601,900),(901,1200),(1201,1700)]
        for i in range(1,6):
            thread=myThread("Thead-"+str(i),link_range=link_range_list[i-1])
            thread.start()
            threads.append(thread)
                # 開始記錄所有行程 (process) 執行完成花費的時間
        start = time.time()
        for p in threads:
            p.join()
        # 執行结果
        total = 0
        while not Que.empty():
            total += Que.get()
        print(total)
        end = time.time()
        print('執行時間: ', (end - start)/60, '分', sep = '')
    def run(self):
        df=self.get_data()
        TF_IDF=self.tf_IDF(df)
        self.time_calculate() 
        df_t=pd.DataFrame(TF_IDF)
        df_t.to_csv(output_path+ 'TF_IDF.csv',encoding='utf-8-sig',index=False)

def main():
    etl_ts=datetime.date.today()
    etl=abt_table()
    etl.run()

if __name__=='__main__':
    main()