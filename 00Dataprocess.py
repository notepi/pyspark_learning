#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19 22:42:26 2018

@author: pan
"""

import pandas as pd
import numpy as np
import os  
from time import time
def file_name(file_dir):  
  for paths, dirs, files in os.walk(file_dir): 
#    print(paths) #当前目录路径 
#    print(dirs) #当前路径下所有子目录 
#    print(files) #当前路径下所有非目录子文件 
    pass
  return paths,dirs,files  

if __name__ == "__main__":
    #是否写入临时文件
    tempfile=1
    #是否是测试
    testflag=1
##############################################################################   
    '''
    文件放在文件中，将文件进行合并
    '''
    #对plan的原始数据做处理
    '''
    1.扫描plan目录，将文件进行合并，数据按照日期进行降序排列
    2.对每种烟，每日的销量进行统计
    '''
    print("=====plandate process===")
    Planfile="./OriginalData/plan"
    print("=====plandate path is:",Planfile)
    Paths,dirs,AllFile = file_name(Planfile)
    
    #对目录进行拼接
    AllFile = [Planfile+'/'+x for x in AllFile]
    #文件名是根据时间命名的，为保证时间点的排序，所以对文件名进行升序排列
    AllFile=sorted(AllFile)
    
    templist=[]
    #对多余的数据进行删除
    for i in AllFile:
        tempdata=pd.read_csv(i)
        del tempdata[u"营销部名称"]
        del tempdata[u"客户名称"]
        del tempdata[u"客户经理"]
        del tempdata[u"经营业态"]
        del tempdata[u"结算方式"]
        templist.append(tempdata)
#        break
        pass
    planAllData = pd.concat(templist)

    #保存时间特征
    date=sorted(planAllData[u"日期"].value_counts().index.tolist())    

    templist=[]
    #整理每日，每一类烟的销售总量进行加和
    for i in date:
        tempdata=planAllData[planAllData[u"日期"]==i]
        tempdata=pd.DataFrame(tempdata.iloc[:,4:].sum()).T
        templist.append(tempdata)
        pass
    planDaySum = pd.concat(templist)
    #添加日期特征
    planDaySum.insert(0,u"日期",date)
    
    if tempfile:
        #每日、每类烟的销售量合并并写入文件中
        planDaySum.to_csv("./TempDate/plan/planDaySum.csv",
                      encoding='GBK',index=False)
        #每个客户，每次针对每种烟的销售量合并并写入文件中
        planAllData.to_csv("./TempDate/plan/planAllData.csv",
                           encoding='GBK',index=False)    
        pass
###############################################################################
    #对real的原始数据做处理
    '''
    1.扫描plan目录，将文件进行合并，数据按照日期进行降序排列
    2.对每种烟，每日的销量进行统计
    '''
    print("=====realdate process===")
    Realfile="./OriginalData/real"
    print("=====realdate path is:",Realfile)
    Paths,dirs,AllFile = file_name(Realfile)
    
    #对目录进行拼接
    AllFile = [Realfile+'/'+x for x in AllFile]
    #文件名是根据时间命名的，为保证时间点的排序，所以对文件名进行升序排列
    AllFile=sorted(AllFile)
    
    templist=[]
    #对多余的数据进行删除
    for i in AllFile:
        tempdata=pd.read_csv(i)
        del tempdata[u"营销部名称"]
        del tempdata[u"客户名称"]
        del tempdata[u"客户经理"]
        del tempdata[u"经营业态"]
        del tempdata[u"结算方式"]
        templist.append(tempdata)
#        break
        pass
    realAllData = pd.concat(templist)

    #保存时间特征
    date=sorted(realAllData[u"日期"].value_counts().index.tolist())    

    templist=[]
    #整理每日，每一类烟的销售总量进行加和
    for i in date:
        tempdata=realAllData[realAllData[u"日期"]==i]
        tempdata=pd.DataFrame(tempdata.iloc[:,4:].sum()).T
        templist.append(tempdata)
        pass
    realDaySum = pd.concat(templist)
    #添加日期特征
    realDaySum.insert(0,u"日期",date)
    
    if tempfile:
        #每日、每类烟的销售量合并并写入文件中
        realDaySum.to_csv("./TempDate/real/realDaySum.csv",
                      encoding='GBK',index=False)
        #每个客户，每次针对每种烟的销售量合并并写入文件中
        realAllData.to_csv("./TempDate/real/realAllData.csv",
                           encoding='GBK',index=False)    
        pass
    
    if testflag:
        realAllData=realAllData.iloc[:50000, ]
        planAllData=planAllData.iloc[:50000, ]
        pass
###############################################################################
    #剔除一些不需要的特征烟
    inout=pd.read_excel('./OriginalData/品牌引入退出时间.xlsx',encoding = "GBK")
    inout=inout[inout[u"退出时间"]!=u"没有数据"]
    
    quitsave=pd.read_csv("./OriginalData/quitsave.csv",encoding='GBK')
    quitsavename=quitsave[u'日期'].tolist()
    
    #完整存在
    fulldate=inout[inout["引入时间"]==20170101]
    fulldate=fulldate[fulldate["退出时间"]==-1]
    fulldatename=fulldate[u'日期'].tolist()
    
    
    #只存在了一段时间、剔除
    middledate=inout[inout["引入时间"]!=20170101]
    middledate=middledate[middledate["退出时间"]!=-1]
    middledatename=middledate[u'日期'].tolist()
    

    #已经退出的、剔除
    quitdate=inout[inout["引入时间"]==20170101]
    quitdate=quitdate[quitdate["退出时间"]!=-1]
    quitdatename=quitdate[u'日期'].tolist()
    
    #新引进的
    newdate=inout[inout["引入时间"]!=20170101]
    newdate=newdate[newdate["退出时间"]==-1]
    newdatename=newdate[u'日期'].tolist()
    
    finalname=fulldatename+newdatename+quitsavename

    if tempfile:
        pd.DataFrame(finalname).to_csv("./dataprocess_dataExtract/finalname.csv",
                    encoding='GBK',index=False)
        pass
    
    if tempfile:
        planDaySum=pd.read_csv("./OriginalData/plan/planDaySum.csv",
                               encoding='GBK')
        realDaySum=pd.read_csv("./OriginalData/real/realDaySum.csv",
                               encoding='GBK')
        fullname=realDaySum.columns.tolist()[1:]
        pass
    fullname=realDaySum.columns.tolist()[1:]
    delname=[i for i in fullname if i not in  finalname]
    
    if tempfile:
        planAllData=pd.read_csv("./OriginalData/plan/planAllData.csv",
                                encoding='GBK')
        realAllData=pd.read_csv("./OriginalData/real/realAllData.csv",
                                encoding='GBK')
        pass
    #根据剔除名，从数据中删除数据
    for i in delname:
        # print(i)
        del planDaySum[i]
        del realDaySum[i]
        del realAllData[i]
        del planAllData[i]
#        break
        pass
    
    if tempfile:
        planDaySum.to_csv("./TempDate/dataprocess_dataExtract/planDaySum.csv",
                          encoding='GBK',index=False)
        realDaySum.to_csv("./TempDate/dataprocess_dataExtract/realDaySum.csv",
                          encoding='GBK',index=False)
        planAllData.to_csv("./TempDate/dataprocess_dataExtract/planAllData.csv",
                           encoding='GBK',index=False)
        realAllData.to_csv("./TempDate/dataprocess_dataExtract/realAllData.csv",
                           encoding='GBK',index=False)
        pass
##############################################################################
    #数据合并、特征衍生
    if tempfile:
        planAllData=pd.read_csv("./TempDate/dataprocess_dataExtract/planAllData.csv",
                                encoding='GBK')
        realAllData=pd.read_csv("./TempDate/dataprocess_dataExtract/realAllData.csv",
                                encoding='GBK')    
        pass
    
    #为数据合并做准备
    #重命名
    #plan的加_paln后缀
    nametemp=planAllData.columns.tolist()
    nametemp[4:]=[i+"_plan" for i in nametemp[4:]]
    planAllData.columns=nametemp
    
    #real的加real后缀
    nametemp=realAllData.columns.tolist()
    nametemp[4:]=[i+"_real" for i in nametemp[4:]]
    realAllData.columns=nametemp

    
    # #对客户购买次数做统计
    # #起始时间
    # start = time()
    # listd=pd.DataFrame([planAllData[u"客户编码"].value_counts().index,
    #                 planAllData[u"客户编码"].value_counts()]).T
    # listd.columns=['ID','times']
    # print("took %.2f seconds for" % ((time() - start)))
    #
    # #将客户的购买和标准进行合并
    # #统计后，每行数据包括用户针对每种烟的真实购买记录和公司定额计划
    # start = time()
    # Datatemp=pd.merge(realAllData,planAllData,
    #             on=[u"日期",u"客户编码"],how='left')
    # print("took %.2f seconds for" % ((time() - start)))
    #
    # #因为两边的都有 档位和购货周期的列，所以合并后会有重复，需要删除
    # #删除合并后重复的字段
    # del Datatemp["档位_y"]
    # del Datatemp["订货周期_y"]
    # '''
    # 内存释放
    # '''
    # realAllData=0
    # planAllData=0
    
