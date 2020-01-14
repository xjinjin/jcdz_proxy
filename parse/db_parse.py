# -*- coding: utf-8 -*-
'''修改数据库结构'''

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

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine) # 创建所有Base派生类所对应的数据表。