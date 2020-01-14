
# -*- coding: utf-8 -*-

########################################
from sqlalchemy import create_engine, Column, Integer, String, LargeBinary,Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SURL = "mysql+pymysql://cic_admin:TaBoq,,1234@192.168.1.170:3306/cicjust_splinter?charset=utf8&autocommit=true"
engine = create_engine(SURL)  # 定义引擎
Base = declarative_base()     # 基类，表都继承这个类
session = sessionmaker(engine)()    # 操作数据库，数据库对话

class JCDFLOWSAVE(Base):
    __tablename__ = 'jcdflowsave'
    id = Column(Integer, primary_key=True)
    now_time = Column(String(40))
    request = Column(LargeBinary())
    response = Column(LargeBinary())
    path = Column(String(100))
    status = Column(Boolean)

# Base.metadata.drop_all(engine)
# Base.metadata.create_all(engine) # 创建所有Base派生类所对应的数据表。

########################################
'''子进程队列通信'''
from multiprocessing import Process, Queue
q = Queue()

def write_queue(q,dicts):
    q.put(dicts)

def read_queue(q):
    while True:
        dicts = q.get(True)
        request = dicts['request']
        response = dicts['response']
        now_time = dicts['now_time']
        try:
            result = JCDFLOWSAVE(now_time=now_time, request=request, response=response, path='',
                                 status=False)  # 创建对象，即表中一条记录
            session.add(result)  # 对象存入数据库
            session.commit()  # 所有的数据处理准备好之后，执行commit才会提交到数据库
        except Exception as e:
            print(e)
            session.rollback()  # 加入数据库commit提交失败，回滚
            res = pickle.loads(response)  # 从字节对象中读取被封装的对象
            path = '../response/{}.txt'.format(now_time)
            with open(path, 'wb') as fw:  # 将数据通过特殊的形式转换为只有python语言认识的字符串，并写入文件
                pickle.dump(res, fw)
            result = JCDFLOWSAVE(now_time=now_time, request=request, path=path, status=False)
            session.add(result)
            session.commit()
########################################
'''进程池异步调用'''
from multiprocessing import Pool
pool = Pool()
# pool.close()
# pool.join()
########################################
'''线程装饰器异步调用'''
from threading import Thread  # thread开启一个新的线程去执行参数fn。
# from threading import Lock
# lock=Lock()
# lock.acquire()
# '''share data'''
# lock.release()
import threading
import os
def async_call(fn):
    def wrapper(*args, **kwargs):
        #通过target关键字参数指定线程函数fun
        thr = Thread(target=fn, args=args, kwargs=kwargs) # 表示控制线程的类。
        # print(thr.isDaemon())    # False
        # thr.setDaemon(True)
        '''如果是后台线程，主线程执行过程中，后台线程也在进行，主线程执行完毕后，后台线程不论成功与否，主线程和后台线程均停止
        如果是前台线程，主线程执行过程中，前台线程也在进行，主线程执行完毕后，等待前台线程也执行完成后，程序停止'''
        thr.start()
        # print(thr.is_alive())   # True
        # thr.join()                # 0.10622406005859375 阻塞当前上下文环境的线程，直到调用此方法的线程终止或到达指定的
    return wrapper

@async_call
def thread_save_data(request, response, now_time):
    '''存flow数据'''
    # print(threading.current_thread().name) # Thread-8
    try:
        result = JCDFLOWSAVE(now_time=now_time, request=request, response=response, path='',status=False)  # 创建对象，即表中一条记录
        session.add(result)  # 对象存入数据库
        session.commit()  # 所有的数据处理准备好之后，执行commit才会提交到数据库
    except Exception as e:
        print(e)
        session.rollback()  # 加入数据库commit提交失败，回滚
        res = pickle.loads(response)  # 从字节对象中读取被封装的对象
        path = '../response/{}.txt'.format(now_time)
        with open(path, 'wb') as fw:  # 将数据通过特殊的形式转换为只有python语言认识的字符串，并写入文件
            pickle.dump(res, fw)
        result = JCDFLOWSAVE(now_time=now_time, request=request, path=path,status=False)
        session.add(result)
        session.commit()
########################################

import pickle
import time

# 中间人
import mitmproxy.addonmanager
import mitmproxy.connections
import mitmproxy.http
import mitmproxy.log
import mitmproxy.proxy.protocol
import mitmproxy.tcp
import mitmproxy.websocket

def save_data(request, response, now_time):
    '''存flow数据'''
    try:
        result = JCDFLOWSAVE(now_time=now_time, request=request, response=response, path='',status=False)  # 创建对象，即表中一条记录
        session.add(result)  # 对象存入数据库
        session.commit()  # 所有的数据处理准备好之后，执行commit才会提交到数据库
    except Exception as e:
        print(e)
        session.rollback()  # 加入数据库commit提交失败，回滚
        res = pickle.loads(response)  # 从字节对象中读取被封装的对象
        path = '../response/{}.txt'.format(now_time)
        with open(path, 'wb') as fw:  # 将数据通过特殊的形式转换为只有python语言认识的字符串，并写入文件
            pickle.dump(res, fw)
        result = JCDFLOWSAVE(now_time=now_time, request=request, path=path,status=False)
        session.add(result)
        session.commit()

links = ['portal/initPortal',
          'edf/org/queryAll',
          '/ba/bankAccount/queryList',
          '/v1/gl/docManage/query',
          '/ba/inventory/queryList',
          '/ba/supplier/queryList',
          '/account/query',
          '/customer/queryList',
          '/v1/ba/person/queryList',
          '/balancesumrpt/query',
          '/balanceauxrpt/query',
          'myip.ipip']

class Proxy():

    def request(self, flow: mitmproxy.http.HTTPFlow):
        '''拦截请求数据'''
        # print('request-start######################')
        # print(os.getpid())  # 25401
        # print(os.getppid()) # 19687
        # print('request-end######################')
        # print(threading.current_thread().name)          # MainThread
        # print(threading.current_thread().getName())     # MainThread
        # print(threading.current_thread().is_alive())    # True
        # print(threading.current_thread().isDaemon())    # False

    def response(self, flow: mitmproxy.http.HTTPFlow):
        '''拦截响应数据'''
        # print('request-start######################')
        # print(os.getpid())  # 25401
        # print(os.getppid()) # 19687
        # print('request-end######################')
        # print(threading.current_thread().name)
        # print(threading.current_thread().getName())
        # print(threading.current_thread().is_alive())
        # print(threading.current_thread().isDaemon())
        for l in links:
            if '/account/queryCalcUsage' in flow.request.url:  # 解决/account/query、/account/queryCalcUsage 相似问题
                pass
            elif l in flow.request.url:
                request = pickle.dumps(flow.request)
                response = pickle.dumps(flow.response)
                now_time = time.strftime('%Y_%m_%d_%H:%M:%S', time.localtime(time.time()))  # 2019_12_25_10:12:10 str

                # start_time = time.time()

                # save_data(request, response, now_time)  # 0.07043313980102539

                thread_save_data(request, response, now_time) # 0.0054090023040771484

                # res = pool.apply_async(save_data, args=(request,response,now_time))  # 0.0003180503845214844
                # pool.close()  # 结束进程池接收任务
                # pool.join()   # 感知进程池中的任务已经执行结束
                # print(res.get())      # 等待进程池内任务都处理完，然后可以用get收集结果。 代理必须结束才行。数据库没数据

                # dicts = {'request':request,'response':response,'now_time':now_time}
                # Process(target=write_queue, args=(q, dicts)).start()
                # Process(target=write_queue, args=(q, dicts)).join()       # 用不起来，代理不是一个函数的方式启动。

                # end_time = time.time()
                # print('run time: {}'.format(end_time - start_time))
                break

addons = [Proxy()]
########################################
if __name__ == '__main__':
    pass
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine) # 创建所有Base派生类所对应的数据表。
