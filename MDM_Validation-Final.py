#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('jupyter nbconvert --to script MDM_Validation-Final.ipynb')


# In[2]:


import os ,sys ,pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType,BooleanType
import json
from pyspark.sql import SQLContext


# In[3]:


spark = SparkSession.builder.appName("MDM_Validation").getOrCreate()
spark


# # Use Below Final Code

# In[6]:


input_file_path="C:/Users/upande/OneDrive - infogroup/Urvashi/PythonScripts/"
output_file_path="C:/Users/upande/OneDrive - infogroup/Urvashi/PythonScripts/"
start_file=input_file_path + "/TL_STARTPROCESS_20220819114400.csv"


schema = StructType([ StructField('FIRST_NAME',StringType(),True), StructField('LAST_NAME',StringType(),True), StructField('MIDDLE_NAME',StringType(),True), StructField('NAME_PREFIX',StringType(),True), StructField('NAME_SUFFIX',StringType(),True), StructField('GENDER',StringType(),True), StructField('DOB',StringType(),True), StructField('ADDR_LINE_1',StringType(),True), StructField('ADDR_LINE_2',StringType(),True), StructField('CITY',StringType(),True), StructField('STATE',StringType(),True), StructField('ZIP',StringType(),True), StructField('ZIP4',StringType(),True), StructField('COUNTRY',StringType(),True), StructField('EMAIL',StringType(),True), StructField('HOME_PHONE',StringType(),True), StructField('WORK_PHONE',StringType(),True), StructField('MOBILE_PHONE',StringType(),True), StructField('COMPANY',StringType(),True), StructField('DIVISION',StringType(),True), StructField('SRC_CUST_NO',StringType(),True), StructField('MISC_01',StringType(),True), StructField('MISC_02',StringType(),True), StructField('MISC_03',StringType(),True), StructField('MISC_04',StringType(),True), StructField('MISC_05',StringType(),True), StructField('MISC_06',StringType(),True), StructField('MISC_07',StringType(),True), StructField('MISC_08',StringType(),True), StructField('MISC_09',StringType(),True), StructField('MISC_10',StringType(),True), StructField('MISC_11',StringType(),True), StructField('MISC_12',StringType(),True), StructField('MISC_13',StringType(),True), StructField('MISC_14',StringType(),True), StructField('MISC_15',StringType(),True), StructField('MISC_16',StringType(),True), StructField('MISC_17',StringType(),True), StructField('MISC_18',StringType(),True), StructField('MISC_19',StringType(),True), StructField('MISC_20',StringType(),True), StructField('CREATED_DATE',StringType(),True) #,StructField("_corrupt_record",StringType(),True)
                    ])

df=spark.read.csv(start_file,header=True)

def read_file(file_name):
    file_path=input_file_path+file_name
    try:
        df_file=spark.read.csv(file_path, header=True)
        return df_file.count()
    except Exception as e:
        return e
    
arr=[]
for file in df.collect()[:1]:
    file_count=read_file(file['FILE_NAME'])
    if type(file_count)==int:
        if file_count==int(file['COUNT']):
            arr.append('True')
            print(file['FILE_NAME'] + " count matches with actual file count :" + str(file_count))
        else:
            arr.append('False')
            print(file['FILE_NAME'] + " count not  matches with actual file count :" + str(file_count))
    else:
        print('ERROR:'+ file['FILE_NAME'] + " not found")
        arr.append('False')
print(arr[0])

#Customer File Validation
if arr[0]=="True":
    print("inside file read")
    raw_df = (spark.read.format("csv")
    .option("header",True)
    .option("delimiter","|")
    .load(input_file_path + "/1_TL_CUST_TEST_20220908081003.txt"))

    with open("valid_layout.json","r") as valid:
    #REading json file from boto3 client
        #val=json.loads(valid)
        val=json.loads(valid.read())
        #print(val["Columns"][0]["Name"])
        for i in val.values():
            for count,item in enumerate(i):
                #print(j["Name"],j.get("length"))
                
                raw_df=raw_df.withColumn(raw_df.columns[count] + "_LEN",
                                             when((length(raw_df.columns[count]) <= item.get("length")) | (length(raw_df.columns[count]).isNull()),"valid")
                                                      .otherwise("invalid_" + raw_df.columns[count] + "_length" ))
                                                  
    ##DOB date format check            
    raw_df_temp=raw_df.withColumn("DOB_val",when(date_format(to_date(raw_df.DOB,"yyyyMMdd"),"yyyyMMdd").isNull(),"invalid DOB")                                   .otherwise("Valid"))
    
    
    ##Type conversion of date columns
    raw_df_temp=raw_df_temp.withColumn("DOB",to_date(raw_df.DOB)).withColumn("CREATED_DATE",to_date(raw_df.CREATED_DATE))

    col = ['FIRST_NAME_LEN',  'LAST_NAME_LEN',  'MIDDLE_NAME_LEN',  'NAME_PREFIX_LEN',  'NAME_SUFFIX_LEN',  'GENDER_LEN',  'DOB_LEN',  'ADDR_LINE_1_LEN',  'ADDR_LINE_2_LEN',  'CITY_LEN',  'STATE_LEN',  'ZIP_LEN',  'ZIP4_LEN',  'COUNTRY_LEN',  'EMAIL_LEN',  'HOME_PHONE_LEN',  'WORK_PHONE_LEN',  'CELL_PHONE_LEN',  'COMPANY_LEN',  'DIVISION_LEN',  'SRC_CUST_NO_LEN',  'MISC_01_LEN',  'MISC_02_LEN',  'MISC_03_LEN',  'MISC_04_LEN',  'MISC_05_LEN',  'MISC_06_LEN',  'MISC_07_LEN',  'MISC_08_LEN',  'MISC_09_LEN',  'MISC_10_LEN',  'MISC_11_LEN',  'MISC_12_LEN',  'MISC_13_LEN',  'MISC_14_LEN',  'MISC_15_LEN',  'MISC_16_LEN',  'MISC_17_LEN',  'MISC_18_LEN',  'MISC_19_LEN',  'MISC_20_LEN',  'CREATED_DATE_LEN',"DOB_val"]


    valid_df_combine=raw_df_temp.withColumn("combine",concat_ws(",",*col))
    
    #Valid data processing 
    valid_df_final= valid_df_combine.filter(~valid_df_combine.combine.rlike("invalid")).drop(*col,"combine")
    
    
    #Reject Data Processing
    #reject_df_final=valid_df_reject.drop(*col,"_corrupt_record").withColumnRenamed("combine","rejectReason").union(invalid_df_temp)
    valid_df_reject= valid_df_combine.filter(valid_df_combine.combine.rlike("invalid"))
    reject_df_final=valid_df_reject.drop(*col,"_corrupt_record").withColumnRenamed("combine","rejectReason")


    print("before writing file")
    # valid_df_final.write.format("csv").mode("overwrite").option("delimiter","|").option("header","True").save("s3://mdm-urvarshi-test/output/1_TL_CUSTOMER_20220801081003_valid.csv")
    # reject_df_final.write.format("csv").mode("overwrite").option("delimiter","|").option("header","True").save("s3://mdm-urvarshi-test/output/1_TL_CUSTOMER_20220801081003_reject.csv")
    

    valid_df_final.coalesce(1).write.format("csv").mode("overwrite").option("delimiter","|").option("header","True").save(output_file_path + "1_TL_CUSTOMER_20220801081003_valid.csv")
    reject_df_final.coalesce(1).write.format("csv").mode("overwrite").option("delimiter","|").option("header","True").save(output_file_path + "1_TL_CUSTOMER_20220801081003_reject.csv")
    

