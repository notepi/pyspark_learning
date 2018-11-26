# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 16:58:08 2018

@author: panpe
"""

import numpy as np
import pandas as pd
import math
import os
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.types import *

if __name__ == "__main__":
    print("======")
    #配置环境
    spark = SparkSession.builder.appName("test").getOrCreate()
    print("ok")

    #读取文件
    # #文件表头
    schema_test = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", FloatType(), True),
        StructField("High", FloatType(), True),
        StructField("Low", FloatType(), True),
        StructField("Close", FloatType(), True),
        StructField("Volume", LongType(), True),
        StructField("Name", StringType(), True)
                       ])
    my_test = spark.read.csv("./data.csv", header=True, schema=schema_test)
    # my_data = spark.createDataFrame(data, schema)
    # spark.stop()
    print("ok")
    pass

