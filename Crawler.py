#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Created on 2013-2-28

@author: ghy459
'''


import sqlite3
import sys
import time
import queue
import threading
import re
import socket

from string import *
from urllib.request import urlopen
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.parse import urlunparse
from bs4 import BeautifulSoup
from posixpath import normpath


Now_pages=[]   ##储存网页内容,从中提取url
Now_pages_url=[]  ##存储网页内容对应的url地址
ToRead_url=[]  ##待访问的网址列表
Readed_url=[]  ##已经访问过的网址列表
Failed_url=[]  ##不能访问的网址列表
LOG=[]         ##日志记录内容列表
Dict={}        ##存放初始参数的字典
Depth=0        ##当前爬虫深度
Depth_Readed_url=0  ##当前深度已读url计数器
g_mutex = threading.Lock() ##线程锁，保证某一时刻只有一个线程对表进行操作
socket.setdefaulttimeout(5) ##设置全局访问超时为5s


Website=''   ##指定网站所在域名
HTML_CHARSET='gb2312'      ##指定网站默认编码



def Start(default):  
    """初始化所有变量"""
    global ToRead_url
    global Website
    global Dict
    
    
    ##把初始参数定义为字典
    Dict={'url':default[0],'deep':default[1],'keyword':default[5],'logfile':default[2],'dbfile':default[4],'loglevel':default[6],'thread':default[3]}
    ##把初始网址置入待爬列表
    ToRead_url.append(Dict['url']) 
    #根据loglevel新建空白日志文件
    if Dict['loglevel'] == 1 :
        title="Time\tUrl\tStatus\n"
    elif Dict['loglevel'] == 2 :
        title="Time\tUrl\tDepth\tStatus\n"
    elif Dict['loglevel'] == 3 :
        title="Time\tUrl\tDepth\tStatus\tKeyword_count\n"
    else :
        title="Time\tUrl\tDepth\tStatus\tKeyword_count\tHTTP_Status\n"
    
    fout=open(Dict['logfile'],'w')
    fout.write(title)
    fout.close()
    #新建空白数据库文件
    fout=open(Dict['dbfile'],'w')
    fout.close()
    #为数据库建立表，结构为（序号，时间，网址，关键词出现次数，网页内容）
    db=sqlite3.connect(Dict['dbfile'])
    yb=db.cursor()
    yb.execute('create table Crawler (id integer primary key,time varchar(20) NOT NULL,url varchar(512) NOT NULL,Keyword_count integer NOT NULL,content NText)')
    db.commit()
    yb.close()
    db.close()
    ##利用正则表达式获取url的根域名
    Website = re.search('([a-z0-9-]){1,63}\.(com|edu.cn|edu|org.cn|me|gov|gov.cn|tk|net|org|cn|co.kr|com.cn)(\:\d+)?$',re.search('(http://)?((\w+\.)+\w+)',Dict['url']).group()).group()
    
    
def Create_LOG(Time,Url,Depth,Status,HTTP_Status,Keyword_count):  
    """给每次url访问生成日志记录"""
    global Dict
    
    ##根据loglevel确定日志内容
    if Dict['loglevel'] == 1 :
        s=Time+"\t"+Url+"\t"+Status+"\n"
    elif Dict['loglevel'] == 2 :
        s=Time+"\t"+Url+"\t"+Depth+"\t"+Status+"\t"+"\n"
    elif Dict['loglevel'] == 3 :
        s=Time+"\t"+Url+"\t"+Depth+"\t"+Status+"\t"+Keyword_count+"\n"
    else :
        s=Time+"\t"+Url+"\t"+Depth+"\t"+Status+"\t"+Keyword_count+"\t"+HTTP_Status+"\n"
        
    return s    ##生成日志记录以string返回    
    '''
    fout=open(Dict['logfile'],'a+')
    fout.write(s)
    fout.close()
    '''


def Write_LOG():
    """将日志记录列表LOG的内容写入日志文件"""
    global LOG
    fout=open(Dict['logfile'],'a+')
    for s in LOG :
        fout.write(s)
    fout.close()
    LOG=[]


def Write_DB(Time,Url,Keyword_count,content):
    """"把数据写入数据库"""
    global Dict
    
    db=sqlite3.connect(Dict['dbfile'], check_same_thread = False)
    yb=db.cursor()
    yb.execute("insert into Crawler(time,url,Keyword_count,content) values(?,?,?,?)",[Time,Url,Keyword_count,content])
    db.commit()
    yb.close()
    db.close()


def do_job(i):
    """定义每个线程需要完成的任务：读取网页内容并查找其中是否包含关键词，若有则写入数据库，最后生成日志记录"""
    global ToRead_url
    global Readed_url
    global LOG
    global Now_pages
    global Now_pages_url
    global HTML_CHARSET
    global Depth
    global Dict
    global g_mutex
    global Depth_Readed_url

    
    url=ToRead_url[i]   ##提取当前所需访问的网址
    Keyword_count = 0   ##定义关键词出现次数变量
    Now_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) ##获取当前时间
    try:
        ##访问url并获取网页内容
        k=urlopen(url)
        
        try:
            charset = k.headers['Content-Type'].split(' charset=')[1].lower()   ##尝试获取网站的字符编码
        except:
            charset = HTML_CHARSET  ##无法获取则使用默认字符编码
        
        url2=k.geturl()  ##geturl()方法获取服务器返回的地址，能够探测到url跳转
        #print (url)
        s=k.read().decode(charset,"ignore") ##网页内容用对应字符解码
    except: 
        ##处理url不能访问的异常
        #print ("Failed to Read : ",url)
        status="Failed"     ##置url状态为Failed，用于生成日志记录
        logstr=Create_LOG(Now_time,url,str(Depth),status,"\t",str(0))   ##调用Create_LOG生成日志记录
        g_mutex.acquire()   ##线程锁——锁上
        LOG.append(logstr)  ##把日志记录加入LOG列表
        Failed_url.append(url)  ##把url加入不能访问列表
        Readed_url.append(url)  ##把url加入已访问列表
        Depth_Readed_url=i  ##已读url计数器+1
        g_mutex.release()   ##线程锁——释放
    else:
        status="Success"    ##置url状态为Success，用于生成日志记录
        
        ##利用BeautifulSoup解析网页内容
        soup=BeautifulSoup(s,from_encoding=charset)   
        if len(Dict['keyword']):    ##判断用户是否有给关键词
            Keyword_count=soup.get_text().count(Dict['keyword'])    ##获取关键词出现次数
            if Keyword_count > 0:   ##如果有关键词出现
                #g_mutex.acquire()   ##线程锁——锁上
                Write_DB(Now_time,url,Keyword_count,s)  ##将网页内容以及相关信息写入DB
                #g_mutex.release()   ##线程锁——释放
        logstr=Create_LOG(Now_time,url,str(Depth),status,str(k.getcode()),str(Keyword_count))   ##调用Create_LOG生成日志记录        
        g_mutex.acquire()   ##线程锁——锁上
        if Depth < Dict['deep'] :   ##若当前url深度已达用户要求，则此url的内容不放入Now_pages中，以节省空间
            Now_pages.append(soup) ##将当前网页内容放入Now_pages，之后从中提取url
            Now_pages_url.append(url)  ##把Now_pages对应的url存放 
        Readed_url.append(url)  ##把url加入已访问列表
        if url2 != url: ##若发生url跳转，则把跳转后的页面也放入已访问列表中
            Readed_url.append(url2)  ##把url加入已访问列表
        LOG.append(logstr)  ##把日志记录加入LOG列表
        Depth_Readed_url=i  ##已读url计数器+1
        g_mutex.release()   ##线程锁——释放
        

class CrawlerPool(object):
    """定义线程池"""
    def __init__(self):
        global ToRead_url
        global Dict
        
        work_num=len(ToRead_url)    ##工作量为需要访问url的条数
        thread_num=Dict['thread']   ##指定线程数量
        
        self.work_queue = queue.Queue()
        self.threads = []
        self.__init_work_queue(work_num)
        self.__init_thread_pool(thread_num)

    """
        初始化线程
    """
    def __init_thread_pool(self,thread_num):
        for i in range(thread_num):
            self.threads.append(Crawler(self.work_queue))

    """
        初始化工作队列
    """
    def __init_work_queue(self, jobs_num):
        for i in list(range(jobs_num)):
            self.add_job(do_job(i), i)

    """
        添加一项工作入队
    """
    def add_job(self, func, *args):
        self.work_queue.put((func, list(args)))#任务入队，Queue内部实现了同步机制
     
    """
        检查剩余队列任务
    """
    def check_queue(self):
        return self.work_queue.qsize()

    """
        等待所有线程运行完毕
    """   
    def wait_allcomplete(self):
        for item in self.threads:
            if item.isAlive():
                item.join()
   
    """
        从网页内容提取url，更新待访问的url列表ToRead_url
    """             
    def UpdateList(self):
        global ToRead_url
        global Readed_url
        global Now_pages
        global Now_pages_url
        global Website
        global HTML_CHARSET
        global Depth_Readed_url
        global Depth
        global Dict
        
        Write_LOG() ##把之前生成的LOG列表写入日志文件中
        ToRead_url=[]   ##更新前的ToRead_url列表里面的url都已经全部访问，所以清空列表
        NewList=[]  ##存放从网页内容提取到的url的列表
        UrlTag='http://'    ##指定提取出来的内容必须带有http://标签
        count = 0 ##Now_pages_url的计数器
        
        if Depth == Dict['deep'] :  ##若当前深度已达用户要求深度，退出程序
            print ("\nDepth is enough ,now exit!")
            sys.exit(0)
        
        print("\n当前深度爬行完毕，等待程序获取新的未访问url列表\n")
        
        
        ##逐条访问之前保存的网页内容
        for s in Now_pages :   
            Base_url = Now_pages_url[count] ##读取Now_pages对应的url
            #Now_parsed = urlparse(Now_url)
            
            ##找到所有Tag为<a></a>的内容
            for link in s.find_all('a'):  
                
                try:
                    k=link.get('href')  ##提取出href的内容，即网页中的可见链接
                except TypeError:   ##上面的k偶尔会抛出NoneType Error，不知道是什么原因
                    continue
                
                ##对于k为相对地址和绝对地址分别进行处理
                ##首先分解提取的url
                url = urlparse(k)
                ##若该url没有协议标识或域名，判定为相对地址，否则为绝对地址
                if url.scheme == "" or url.netloc == "" :
                    temp = urljoin(Base_url,k)  ##把基址与相对地址合并
                    ##以下步骤为防止http://www.xx.com/../../abc.html出现
                    arr = urlparse(temp)    ##分解刚合并的地址
                    path = normpath(arr[2]) ##规范化其中的路径部分
                    ##再次合并网址
                    New_url = urlunparse((arr.scheme, arr.netloc, path, arr.params, arr.query, arr.fragment))
                    try:
                        NewList.append(New_url) ##把网址加入到New_list中   
                    except AttributeError:  ##上面提取出的url可能为空，属性为NoneType，所有没有find()方法，抛出异常
                        continue
                    
                else :
                    ##若提取出来的绝对地址是同域的，且是第一次被获取，那么放入New_List中
                    try:
                        if (k.find(Website) != -1) and (k not in NewList) :
                            NewList.append(k)   ##把网址加入到New_list中 
                    except AttributeError:  ##上面提取出的url可能为空，属性为NoneType，所有没有find()方法，抛出异常
                        continue 
            count += 1  ##计数器+1
                        
        ##对比已访问过的列表，从NewList中找出未访问的地址，放入ToRead_url中
        ToRead_url=list(set(NewList) - set(Readed_url))
        ##将当前深度+1
        
        Depth += 1
        Now_pages=[] ##清空网页内容
        Depth_Readed_url = 0  ##清空当前已读url数量计数器
        
        if ToRead_url == [] :   ##待访问列表为空，退出程序；否则继续
            print ("\nAll urls are read,exit!")
            sys.exit(0)

            
class Crawler(threading.Thread):
    """定义爬虫线程"""
    def __init__(self, work_queue):
        threading.Thread.__init__(self)
        self.work_queue = work_queue
        self.start()

    def run(self):
        #死循环，从而让创建的线程在一定条件下关闭退出
        while True:
            try:
                do, args = self.work_queue.get(block=False)#任务异步出队，Queue内部实现了同步机制
                do(args)
                self.work_queue.task_done()#通知系统任务完成
            except Exception :
                #print ("")
                break


class Crawler_Status(threading.Thread):
    """定义输出当前状态线程"""
    def __init__(self):
        threading.Thread.__init__(self)
        self.thread_stop = False

    def run(self):
        """死循环，从而让创建的线程不结束"""
        
        global ToRead_url
        global Failed_url
        global Depth
        global g_mutex
        global Depth_Readed_url
        
        while not self.thread_stop:
            #g_mutex.acquire()   ##线程锁——锁上
            ToRead_len=len(ToRead_url)
            Failed_len=len(Failed_url)
            
            print("当前深度为: %d, 此深度已访问url数量: %d, 待访问url数量: %d, 无法访问url数量: %d" % (Depth,Depth_Readed_url,(ToRead_len-Depth_Readed_url),Failed_len))
            #g_mutex.release()   ##线程锁——释放
            time.sleep(10)
            
    def stop(self):
        global ToRead_url
        global Readed_url
        global Failed_url
        ToRead_len=len(ToRead_url)
        Readed_len=len(Readed_url)
        Failed_len=len(Failed_url)
        print("\n当前深度所有url读取完成, 访问url总数量: %d, 访问当前深度url数量: %d, 无法访问url数量: %d" % (Readed_len,ToRead_len,Failed_len))
        self.thread_stop = True
