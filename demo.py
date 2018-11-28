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
from time import time
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":
    print("======")
    # 配置环境
    spark = SparkSession.builder.appName("test").getOrCreate()
    print("ok")

    # 读取文件
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
    one = my_test[my_test.columns[1:]]
    # 创建全局表格
    my_test.createGlobalTempView("people")

    start = time()
    two_test = spark.sql("select * from global_temp.people")
    print("took %.2f seconds for" % ((time()-start )))

    # 将数据转换成标签数据
    def parsePoint(line):
        return (line[-1], Vectors.dense(line[:-1]))
    # # parsedData = my_test.map(parsePoint)
    parsedData = my_test.rdd.map(parsePoint)
    df = spark.createDataFrame(parsedData, ["label", "features"])
    hasattr(parsedData, "toDF")
    # df = spark.createDataFrame(parsedData, ["label", "features"])
    # 读取spark的训练格式


    # $example on$
    # Load the data stored in LIBSVM format as a DataFrame.
    demo_data_libsvm = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")







    # my_data = spark.createDataFrame(data, schema)
    # spark.stop()
    print("ok")
    pass

