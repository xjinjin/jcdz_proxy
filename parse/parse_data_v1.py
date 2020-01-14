
# -*- coding: utf-8 -*-

########################################
from sqlalchemy import create_engine, Column, Integer, String, LargeBinary,Boolean
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SURL = "mysql+pymysql://cic_admin:TaBoq,,1234@192.168.1.170:3306/cicjust_splinter?charset=utf8&autocommit=true"
engine = create_engine(SURL)  # 定义引擎
Base = declarative_base()     # 基类，表都继承这个类
session = sessionmaker(engine)()    # 操作数据库，数据库对话

class JCDZT(Base):
    __tablename__ = 'jcdzt'
    id = Column(Integer, primary_key=True)
    mobile = Column(String(40))
    ztname = Column(String(40))
    token = Column(String(1000))
    orgid = Column(String(30))
    start = Column(String(30))
    finshed = Column(String(30))
    status = Column(String(40))

class JCDEXPORT(Base):
    __tablename__ = 'jcdexport'
    id = Column(Integer, primary_key=True)
    mobile = Column(String(20))
    label = Column(String(20))
    ztname = Column(String(20))
    data = Column(LONGTEXT)
    kjqj = Column(String(20))

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
import json
import pickle
import time
import pandas
########################################
null = ''
true = ''
false = ''

class classifly:

    def __init__(self):
        self.links = {'login': 'portal/initPortal',
                      'qyxx': 'edf/org/queryAll',
                      'zhanghu': '/ba/bankAccount/queryList',
                      'cunhuo': '/ba/inventory/queryList',  # 大部分客户未开通此功能，基本无此url
                      'gongyinshang': '/ba/supplier/queryList',
                      'kjkm': '/account/query',
                      'kehu': '/customer/queryList',
                      'bmry': '/v1/ba/person/queryList',
                      'pingzheng': '/v1/gl/docManage/query',
                      'yeb': '/balancesumrpt/query',
                      'fzyeb':'/balanceauxrpt/query'}

    def fetch_sql(self):
        '''从数据库里取数据'''
        while True:
            data = pandas.read_sql("select * from jcdflowsave where status = 0", engine)
            if len(data.index) == 0:
                time.sleep(10)  # 睡600秒
            else:
                for i in data.index.values:  # 获取行号的索引，并对其进行遍历：
                    # 根据i来获取每一行指定的数据 并利用to_dict转成字典
                    row_data = data.loc[i, ['id', 'now_time', 'request', 'response', 'path']].to_dict()
                    print(row_data['id'])
                    session.query(JCDFLOWSAVE).filter(JCDFLOWSAVE.id == int(row_data['id'])).update({'status': 1})
                    session.commit()
                    self.databag(row_data)

    def databag(self, row_data):
        '''处理url'''
        flow_request = pickle.loads(row_data['request'])
        try:
            flow_response = pickle.loads(row_data['response'])
        except TypeError as e:
            path=row_data['path']
            with open(file=path,mode='rb')  as f:
                result=f.read()
                flow_response=pickle.loads(result)
        for label, link in self.links.items():
            if link in flow_request.url:
                self.deal_data(flow_request, flow_response, label)
                break

    def deal_data(self, *args):       # 处理需要的数据，有效的url
        '''处理数据'''
        flow_request, flow_response, label= args
        if 'login' == label:
            self.login(flow_request, flow_response, label)
        elif 'pingzheng' == label:
            self.pingzheng(flow_request, flow_response, label)
        elif 'yeb' == label:
            self.yeb(flow_request, flow_response, label)
        elif 'fzyeb' == label:
            self.fzyeb(flow_request, flow_response, label)
        elif 'qyxx' == label:
            self.qyxx(flow_request, flow_response, label)
        elif 'zhanghu' == label:
            self.zhanghu(flow_request, flow_response, label)
        elif 'cunhuo' == label:
            self.cunhuo(flow_request, flow_response, label)
        elif 'gongyinshang' == label:
            self.gongyinshang(flow_request, flow_response, label)
        elif 'kjkm' == label:
            self.kjkm(flow_request, flow_response, label)
        elif 'kehu' == label:
            self.kehu(flow_request, flow_response, label)
        elif 'bmry' == label:
            self.bmry(flow_request, flow_response, label)

    def login(self, *args):
        '''解析login'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        user = eval(flow_response.text)['value']['user']   # dict
        org = eval(flow_response.text)['value']['org']
        mobile = user['mobile']
        ztname = org['name']
        orgid = str(org['id'])   # int 解决数值类型时报错，超出范围问题
        dicts = dict(token=token, mobile=mobile, label=label, ztname=ztname, orgid=orgid)
        if self.search_data(dicts):         # 根据token判断此公司是否在JCDZT数据库中    1.pass    2.添加到JCDZT
            pass
        else:
            self.add_data(dicts)

    def bmry(self, *args):
        '''处理部门人员信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)
        if self.search_data(dicts):
            self.update_data(dicts)
        else:
            self.add_data(dicts)

    def kjkm(self, *args):
        '''处理会计科目信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)
        if self.search_data(dicts):
            self.update_data(dicts)
        else:
            self.add_data(dicts)

    def kehu(self, *args):
        '''处理客户信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)
        if self.search_data(dicts):
            self.update_data(dicts)
        else:
            self.add_data(dicts)

    def cunhuo(self, *args):
        '''处理存货信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)
        if self.search_data(dicts):
            self.update_data(dicts)
        else:
            self.add_data(dicts)

    def gongyinshang(self, *args):
        '''处理供应商信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)
        if self.search_data(dicts):
            self.update_data(dicts)
        else:
            self.add_data(dicts)

    def zhanghu(self, *args):
        '''处理账户信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']  # dict
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)
        if self.search_data(dicts):
            self.update_data(dicts)
        else:
            self.add_data(dicts)

    def qyxx(self, *args):
        '''处理企业信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        data = eval(flow_response.text)['value']
        dicts = dict(data=data, token=token, label=label)
        dicts = self.fetch_data(dicts)      # 补：ztname,mobile
        if self.search_data(dicts):         # 判断数据库中是否有此公司的企业信息
            self.update_data(dicts)         # 用现在的数据更新数据库
        else:
            self.add_data(dicts)   # 没有则添加

    def fzyeb(self, *args):  # 每个月的值
        '''处理余额表'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        beginPeriod = eval(flow_request.content)['beginPeriod']  # dict,str
        beginYear = eval(flow_request.content)['beginYear']  # dict,str
        date = '{}-{}'.format(beginYear,beginPeriod)
        data = eval(flow_response.text)['value']    # dict
        dicts = dict(data=data, token=token, label=label, date=date)
        dicts = self.fetch_data(dicts)
        self.fzyeb_data(dicts)

    def fzyeb_data(self, dicts):
        '''处理余额表信息'''
        result = session.query(JCDEXPORT).filter(JCDEXPORT.kjqj == dicts['date'],
                                                 JCDEXPORT.ztname == dicts['ztname'],
                                                 JCDEXPORT.mobile == dicts['mobile'],
                                                 JCDEXPORT.label == dicts['label']).first()
        if result:
            session.query(JCDEXPORT).filter(JCDEXPORT.kjqj == dicts['date'], JCDEXPORT.ztname == dicts['ztname'],
                                              JCDEXPORT.mobile == dicts['mobile'], JCDEXPORT.label == dicts['label']
                                              ).update({'data': json.dumps(dicts['data'])})
        else:
            result = JCDEXPORT(mobile=dicts['mobile'], label=dicts['label'], ztname=dicts['ztname'],
                               data=json.dumps(dicts['data']), kjqj=dicts['date'])
            session.add(result)
        session.commit()

    def yeb(self, *args):  # 每个月的值
        '''处理余额表'''
        flow_request, flow_response, label= args
        token = dict(flow_request.headers)['token']
        beginDate = eval(flow_request.content)['beginDate']  # dict,str
        data = eval(flow_response.text)['value']    # dict
        dicts = dict(data=data, token=token, label=label, beginDate=beginDate)
        dicts = self.fetch_data(dicts)
        self.yeb_data(dicts)

    def yeb_data(self, dicts):
        '''处理余额表信息'''
        result = session.query(JCDEXPORT).filter(JCDEXPORT.kjqj == dicts['beginDate'],
                                                 JCDEXPORT.ztname == dicts['ztname'],
                                                 JCDEXPORT.mobile == dicts['mobile'],
                                                 JCDEXPORT.label == dicts['label']).first()
        if result:
            session.query(JCDEXPORT).filter(JCDEXPORT.kjqj == dicts['beginDate'], JCDEXPORT.ztname == dicts['ztname'],
                                              JCDEXPORT.mobile == dicts['mobile'], JCDEXPORT.label == dicts['label']
                                              ).update({'data': json.dumps(dicts['data'])})
        else:
            result = JCDEXPORT(mobile=dicts['mobile'], label=dicts['label'], ztname=dicts['ztname'],
                               data=json.dumps(dicts['data']), kjqj=dicts['beginDate'])
            session.add(result)
        session.commit()

    def pingzheng(self, *args):
        '''处理凭证信息'''
        flow_request, flow_response, label = args
        token = dict(flow_request.headers)['token']
        results = eval(flow_response.text)['value']['dtoList']
        if results:
            container = {}
            for i in results:   # i 是一行凭证数据，字典形式
                voucherDate = i['voucherDate']
                if voucherDate not in container:
                    container['{}'.format(voucherDate)] = [i]
                else:
                    box = container['{}'.format(voucherDate)]
                    box.append(i)
                    container['{}'.format(voucherDate)] = box
            dicts = dict(box=container, token=token, label=label)
            dicts = self.fetch_data(dicts)      # 字典 补充账套名，电话
            self.pingzheng_data(dicts)

    def pingzheng_data(self, dicts):
        '''处理凭证数据'''
        box = dicts['box']
        for i in box:
            kjqj = i                # 日期:'2019-11-30'
            infodata = box[kjqj]    # 每个日期的列表有多个字典凭证
            result = session.query(JCDEXPORT).filter(JCDEXPORT.kjqj == i,
                                                     JCDEXPORT.ztname == dicts['ztname'],
                                                     JCDEXPORT.mobile == dicts['mobile'],
                                                     JCDEXPORT.label == dicts['label']).first()
            if result:
                session.query(JCDEXPORT).filter(JCDEXPORT.kjqj == i,JCDEXPORT.ztname == dicts['ztname'],
                                                  JCDEXPORT.mobile == dicts['mobile'],JCDEXPORT.label == dicts['label']
                                                  ).update({'data': json.dumps(infodata)})
            else:
                result = JCDEXPORT(mobile=dicts['mobile'], label=dicts['label'], ztname=dicts['ztname'],
                                   data=json.dumps(infodata), kjqj=kjqj)
                session.add(result)
            session.commit()

    def fetch_data(self, dicts):
        '''获取数据'''
        result = session.query(JCDZT).filter(JCDZT.token == dicts['token']).all()
        for i in result:
            dicts['mobile'] = i.mobile
            dicts['ztname'] = i.ztname
        return dicts

    def search_data(self, dicts):
        '''查找数据'''
        if 'login' in dicts.values():
            return session.query(JCDZT).filter(JCDZT.token == dicts['token']).first()
        else:
            return session.query(JCDEXPORT).filter(JCDEXPORT.mobile == dicts['mobile'],
                                                   JCDEXPORT.ztname == dicts['ztname'],
                                                   JCDEXPORT.label == dicts['label']).first()

    def update_data(self, dicts):
        '''更新数据'''
        session.query(JCDEXPORT).filter(JCDEXPORT.ztname == dicts['ztname'],
                                        JCDEXPORT.mobile == dicts['mobile'],
                                        JCDEXPORT.label == dicts['label']
                                        ).update({'data': json.dumps(dicts['data'])})
        session.commit()

    def add_data(self, dicts):
        '''增加数据'''
        if 'login' in dicts.values():
            result = JCDZT(mobile=dicts['mobile'], ztname=dicts['ztname'], token=dicts['token'],
                           orgid=dicts['orgid'], start='', finshed='', status='')
        else:
            result = JCDEXPORT(mobile=dicts['mobile'], label=dicts['label'], ztname=dicts['ztname'],
                               data=json.dumps(dicts['data']),
                               kjqj='')
        session.add(result)
        session.commit()

if __name__ == '__main__':
    CLASSIFLY = classifly().fetch_sql()
########################################
