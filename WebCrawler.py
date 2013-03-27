#!/usr/bin/python3
# -*- coding: UTF-8 -*-
'''
Created on 2013-2-27

@author: ghy459
'''

import sys
import Crawler
import time
from optparse import OptionParser


def Usage_Option():
    
    usage = "%prog [options] -u url -d deep -f logfile [ -l loglevel(1-5)  | --testself ] -thread number --dbfile filepath --key 'HTML5'"
    parser = OptionParser(usage,version="WebCrawler v1.2")    
    parser.add_option("-u","--url",type="string",dest="url",help="指定爬虫开始地址")
    parser.add_option("-d","--deep",type="int",dest="deep",help="指定爬虫深度")
    parser.add_option("-f","--file",type="string",dest="logfile",default="spider.log",help="指定日志记录文件,默认spider.log")
    parser.add_option("--thread",type="int",dest="thread",default="10",help="指定线程池大小，多线程爬取页面，可选参数，默认10")
    parser.add_option("--dbfile",type="string",dest="dbfile",default="spider.db",help="存放结果数据到指定的数据库（sqlite）文件中,默认spider.db")
    parser.add_option("--key",type="string",dest="keyword",default="",help="页面内的关键词，获取满足该关键词的网页，可选参数，默认为所有页面")
    parser.add_option("-l","--loglevel",type="int",dest="loglevel",default="1",help="日志记录文件记录详细程度，数字越大记录越详细，范围1~4,可选参数,默认为1")
    parser.add_option("--testself",help="程序自测，可选参数")

    (options, args) = parser.parse_args()

    URL = ""
    DEEP = 0
    LOGFILE = "spider.log"
    THREAD = 10
    DBFILE = "spider.db"
    KEYWORD = ""
    LOGLEVEL = 1  
    
    print()  
    
    if options.url == "" :
        parser.error("No url input!")
        sys.exit()
    else:
        URL = options.url
 
    if options.deep == 0:
        parser.error("No deep input!")
        sys.exit()
    else:
        DEEP=options.deep
        
    if options.logfile == "" :
        print ("No log filepath input,default logfile is spider.log")
        
    else:
        LOGFILE=options.logfile

    if options.thread == "" :
        print ("No thread input,default thread is 10")
        
    else:
        THREAD=options.thread  
          
    if options.dbfile == "" :
        print ("No dbfile input,default defile is spider.db")
        DBFILE="spider.db"
    else:
        DBFILE=options.dbfile 
    
    if options.loglevel == "" :
        print ("No loglevel input,default loglevel is 1")
        LOGLEVEL=1
    else:
        if options.loglevel > 4 :
            print ("loglevel too large,now loglevel is 3")
            LOGLEVEL=4
        elif options.loglevel < 1:
            print ("loglevel too small,now loglevel is 1")
            LOGLEVEL=1
        else :
            LOGLEVEL=options.loglevel
    
    KEYWORD = options.keyword
    
    print()
    
    return [URL,DEEP,LOGFILE,THREAD,DBFILE,KEYWORD,LOGLEVEL]


def PRINT_DEFAULT_DATA(DATA):
    """打印初始化信息"""
    print ("***************DEFAULT DATA***************")
    print ("The url is : %s" % DATA[0])
    print ("The deep is : %s" % DATA[1])
    if DATA[5] == "" :
        print ("The keyword is : (NO KEYWORD!)")
    else :
        print ("The keyword is : %s" % DATA[5])
    print ("The logfile is : %s" % DATA[2])
    print ("The dbfile is : %s" % DATA[4])
    print ("The loglevel is : %s" % DATA[6])
    print ("The thread is : %s" % DATA[3])
    print ("******************************************")
    print()
    
    
if __name__ == '__main__':
    

    DEFAULT=Usage_Option()  ##获取用户输入信息
    PRINT_DEFAULT_DATA(DEFAULT) ##打印初始信息
    
    print("WebCrawler will start in 3 sec...")
    time.sleep(3)   ##等待3秒后执行程序
    print()
    Crawler.Start(DEFAULT)  ##将初始信息传到另一个文件
    
    
    
    while(1) :
        Status=Crawler.Crawler_Status()
        Status.setDaemon(True)
        Status.start()
        Web_Crawler = Crawler.CrawlerPool() ##创建线程池实例
        Web_Crawler.wait_allcomplete()  ##等待让线程池完成的所有工作结束
        Status.stop()
        Web_Crawler.UpdateList()    ##更新待访问列表
        
    
    