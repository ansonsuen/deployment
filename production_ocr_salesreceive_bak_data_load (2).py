#!/usr/bin/env python
# coding: utf-8

# In[1]:



class common_function:
    def generate_require_field_list(self, df_required_data):
        dic = {}
        for i in range(len(df_required_data)):
            dic[df_required_data["Field Name / Label"].iloc[i]] = df_required_data["data_type"].iloc[i]
        return dic
    def removepunc( st):
#    punctuation= '''!()[]{};'"\,<>./?@#$%^&*_~'''
#    newString=""
#    for x in st:
#        if x not in punctuation or x == " ":
#            newString=newString+x
#    return newString
        newString=MySQLdb.escape_string(st)
        if newString[-1] == '\\':
            newString+='\\'
        return newString
    def generate_mysql_insertstatement(self, df, dic, db, table_name, path):
        import re
        import pandas as pd

        alist = []
        blist=[]
        txt_file = open(path,"w+", encoding="utf-8")
        string = ""

        txt_file.write("BEGIN; \n")
        for i in range(len(df)):
            string = "INSERT INTO {db}.{name} (".format(db = db, name = table_name)
            for c in df.columns:
                if c != df.columns[-1]:
                    string = string + c + ", "
                else:
                    string = string + c 
            string = string + ") "
            string = string + "VALUES "
            string = string + "("
            for c in df.columns:

                blist.append(df[c].iloc[i]) 
                    # print(df[c].iloc[i])
                    # print("************")
                    # print(dic.get(c))
                    # print(c)
                if c != list(df.columns)[-1]:
                    # print(df[c].iloc[i])
                    # print(type(df[c].iloc[i]))
                    # print(pd.isna(df[c].iloc[i]))

                    if str(df[c].iloc[i])=="<NA>" or pd.isna(df[c].iloc[i]) == True:
                        string = string + 'NULL'  + ", "
                    elif re.search("NUM", dic.get(c)) is not None :
                        string = string + str(float(df[c].iloc[i])) + ", "
                    elif re.search("float", dic.get(c)) is not None :
                        string = string + str(float(df[c].iloc[i])) + ", "
                    else:
                        try:
                            df[c].iloc[i] = common.removepunc(df[c].iloc[i])
                        except:
                            pass
                        string = string + "'" + str(df[c].iloc[i]).replace("'","\\'") + "'" + ", "
                else:
                    if  str(df[c].iloc[i])=="<NA>" or pd.isna(df[c].iloc[i]) == True:
                        string = string + 'NULL'  
                    elif re.search("NUM", dic.get(c)) is not None :
                        string = string + str(float(df[c].iloc[i]))
                    elif re.search("float", dic.get(c)) is not None :
                        string = string + str(float(df[c].iloc[i])) + ", "
                    else:
                        try:
                            df[c].iloc[i] = common.removepunc(df[c].iloc[i])
                        except:
                            pass
                        string = string + "'" + str(df[c].iloc[i]).replace("'","\\'") + "'" 
                        string = string.replace("'nan'", "NULL")
                
            string = string + ") "
            string = string.replace("'nan'", "NULL")
            string = string.replace("nan", "NULL")
            string = string + ";"
            txt_file.write(string+"\n")
        txt_file.write("COMMIT; \n")

        return string
        
        


# In[2]:


# import csv 
# import pandas as pd 
# path = '/Users/ansonsuen/Desktop/untitled folder/' 
# file = 'SALESRECEIVE_BAK.csv' 
# f = open(path+file,'rt') 
# reader = csv.reader(f) 
# print(reader)
# csv_list = [] 
# for l in reader: 
#     csv_list.append(l) 
# f.close() 
# df = pd.DataFrame(csv_list)
# print(df)
# header_row = 1

# df.columns = df.iloc[0] 
# df = df.drop(0)

# df


# In[3]:


import pandas as pd
import datetime
import numpy as np
import csv
import MySQLdb
import re
#data dict
path = "/Users/ansonsuen/Desktop/untitled folder/K11_CRM_OCR_RAW copy_exmple (1).csv"
print("#################   start read the schema excel    ###########################################")
df_required_data = pd.read_csv(path)
common = common_function()
dic = common.generate_require_field_list(df_required_data)
database_name = "crm" # or crm
path = "/Users/ansonsuen/Downloads/production_data_load-14-02-2022 2"
table_name = "ocr_matching"
txt_path = "/Users/ansonsuen/Downloads/production_data_load-14-02-2022 2/k11_crm_ocr_raw_config.txt"
#clonedb
txn_name = "/Users/ansonsuen/ocr_matching.csv"
df = pd.read_csv(txn_name, sep=',' , dtype= "string",engine='python',encoding='UTF-8')


# In[4]:


df


# In[5]:


required_col = df_required_data["Field Name / Label"].to_list()
dic


# In[6]:




for key, value in dic.items():
    if re.search("DATETIME", value.upper()) is not None or re.search("DATE", value.upper()) is not None:
        print(key)


# In[7]:



dic_schema = {}
for key, value in dic.items():
    if re.search("NUM", value.upper()) is not None:
        dic_schema[key] = "float64"
    else:
        dic_schema[key] = "string"

additional_col = [x for x in required_col if x not in df.columns]
print(additional_col)


# In[8]:


dic_schema


# In[9]:


df.columns


# In[10]:


df['INSERTDATE']


# In[11]:


df["TRANSACTION_ID"] = np.nan
df["XF_BONUS"] = np.nan
df["PARKING_IND"] = np.nan
df["RANDOM_CHECK_IND"] = np.nan
df["DUPLICATE_IND"] = np.nan
df["DUPLICATE_RULE_ID"] = np.nan
df["REJECT_CODE"] = df["FEEDBACKINFO"]
df["XF_GRADE"] = df["GRADE"]
df["XF_CONFIRMBY"] = df["LASTMODIFYUSER"]
df["XF_CONFIRMDATE"] = df["LASTMODIFYDATE"]
df["COMPLETED_DATE"] = df["LASTMODIFYDATE"]
df["NEW_DATA_IND"] = "0"
df["XF_VOIDSTATUS"] = "0"
df["XF_VOIDREASON"] = np.nan


# In[12]:


df = df.reset_index()


# In[13]:


today = datetime.datetime.now()
date_str = today.strftime("%Y%m%d%H%M%S%f")
df["index"] = df["index"].astype("str")
df["TRANSACTION_ID"] = df.apply(lambda x: date_str + x["index"] if pd.isna(x["RECORD_ID"]) == True else x["RECORD_ID"], axis=1)


# In[14]:


for k, v in dic_schema.items():
    print(k)
    if k !="AMT" and k !="KDOLLAR":
        df[k] = df[k].astype(v)
# for col in df_store.columns:
#     print(col)
#     df_store[col] = df_store[col].astype(dic_schema.get(col))
df_tmp = df.copy()
for c in df_tmp.columns:
    if c not in required_col:
        df_tmp.drop(columns= c, axis=1, inplace=True)

df_tmp.fillna(np.nan, inplace=True)
df_tmp = df_tmp.replace("<NA>",np.nan) 


# In[15]:


df_tmp.loc[df_tmp['MEMONO'].str.endswith("\\").replace('\\','\\\\')]


# In[16]:


df_tmp['MEMONO']


# In[17]:


df_tmp['MEMONO']


# In[18]:


df_tmp.loc[df_tmp['BATCHRECORDID']=='FZHKCRMA9A05F6A68A34A6FB2E956086AA0A80F']


# In[19]:


df_tmp['LASTMODIFYDATE']


# In[20]:


len(df_tmp)


# In[21]:


df_list = []
from multiprocessing import Process, Value
import os.path
number_of_row = 100000
number_of_df = int(np.ceil((len(df_tmp)/number_of_row)))

all_function_list = []

for i in range(1,number_of_df+1):
    start_row = (i-1)*number_of_row
    end_row = i*number_of_row
    function_list = []

    #path = "/Users/ansonsuen/Downloads/production_data_load-14-02-2022 2"
    file_path = os.path.join(path,"K11_CRM_OCR_INSERT_{}.txt".format(i))
    #table_name = "K11_CRM_OCR_RAW"
    target_file_path = os.path.join(path,"K11_CRM_OCR_{}.parquet".format(i))
    df_tmp[start_row:end_row].to_parquet(target_file_path,index=False)
    function_list.append(target_file_path)
    function_list.append(dic_schema)
    function_list.append(database_name )
    function_list.append(table_name)
    function_list.append(file_path)
    all_function_list.append(function_list)


# In[22]:


dic_schema


# In[23]:


#txt_path = "/Users/ansonsuen/Downloads/production_data_load-14-02-2022 2/k11_crm_ocr_raw_config.txt"
import ast
with open(txt_path,"w+", encoding="utf-8") as f:
    f.write(str(all_function_list))
    f.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




