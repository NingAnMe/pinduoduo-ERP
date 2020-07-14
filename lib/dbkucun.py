#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-14 17:37
# @Author  : NingAnMe <ninganme@qq.com>
from datetime import datetime

import pandas as pd
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import relationship

from lib.database import *


class Goods(Base):
    __tablename__ = 'goods'

    # 商品编码
    bianma = Column(String, primary_key=True)
    mingcheng = Column(String)
    danwei = Column(String)
    shuliang = Column(Integer, nullable=False, default=0)
    junjia = Column(Float, nullable=False, default=0)
    beizhu = Column(String)
    gengxinshijian = Column(DateTime)

    goods_relate_sku = relationship('Sku', backref='sku_relate_goods')

    @classmethod
    def add(cls, session, goods):
        goods = Goods(**goods)
        goods.gengxinshijian = datetime.now()
        return session.add(goods)

    @classmethod
    def query(cls, session):
        return session.query(Goods).all()

    @classmethod
    def query_bianma_in_(cls, session, bianmas):
        return session.query(Goods).filter(Goods.bianma.in_(bianmas)).all()


class Sku(Base):
    __tablename__ = 'sku'

    id = Column(Integer, primary_key=True, autoincrement=True)
    bianma = Column(String, nullable=False)
    mingcheng = Column(String)
    danwei = Column(String, default='件')
    shuliang = Column(Integer, nullable=False)
    beizhu = Column(String)
    gengxinshijian = Column(DateTime)

    goods_bianma = Column(String, ForeignKey("goods.bianma"))

    @classmethod
    def add(cls, session, sku):
        sku = Sku(**sku)
        sku.gengxinshijian = datetime.now()
        return session.add(sku)

    @classmethod
    def query(cls, session):
        return session.query(Sku).all()

    @classmethod
    def query_bianma(cls, session, bianmas):
        return session.query(Sku).filter(Sku.bianma == bianmas.strip()).all()


class Order(Base):
    __tablename__ = 'order'

    # 商品 订单号 订单状态  商品数量(件) 是否审核中
    shangpin = Column(String)
    dingdanhao = Column(String, primary_key=True)
    dingdanzhuangtai = Column(String)
    shangpinshuliang = Column(Integer)
    shifoushenhezhong = Column(String)

    # 支付时间 拼单成功时间 订单确认时间 承诺发货时间 发货时间 确认收货时间
    zhifushijian = Column(DateTime)
    pindanchenggongshijian = Column(DateTime)
    dingdanquerenshijian = Column(DateTime)
    chengnuofahuoshijian = Column(DateTime)
    fahuoshijian = Column(DateTime)
    querenfahuoshijian = Column(DateTime)

    # 商品id 商品规格 样式ID 商家编码-SKU维度 商家编码-商品维度
    shangpinid = Column(String)
    shangpinguige = Column(String)
    yangshiid = Column(String)
    skubianma = Column(String)
    shangpinbianma = Column(String)

    # 商家备注 买家留言 售后状态
    shangjiabeizhu = Column(String)
    maijialiuyan = Column(String)
    shouhouzhuangtai = Column(String)

    # 商品总价(元) 店铺优惠折扣(元) 平台优惠折扣(元) 邮费(元) 用户实付金额(元) 商家实收金额(元)
    shangpinzongjia = Column(Float)
    dianpuyouhuizhekou = Column(Float)
    pingtaiyouhuizhekou = Column(Float)
    youfei = Column(Float)
    yonghushifujine = Column(Float)
    shangjiashishoujine = Column(Float)

    # 快递单号 快递公司 收货人 手机 省 市 区 详细地址
    kuaididanhao = Column(String)
    kuaidigongsi = Column(String)
    shouhuoren = Column(String)
    shouji = Column(String)
    sheng = Column(String)
    shi = Column(String)
    qu = Column(String)
    xiangxidizhi = Column(String)

    # 是否抽奖或0元试用
    shifouchoujianghuoshiyong = Column(String)

    # 是否去库存 更新时间
    shifouqukucun = Column(String, nullable=False, default='否')
    gengxinshijian = Column(DateTime)

    @staticmethod
    def str2date(date_str):
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except Exception as why:
            return datetime(1970, 1, 1)

    @classmethod
    def csv2order(cls, csv_file):
        """
        只处理已经发货的订单
        :param csv_file:
        :return:
        """
        pd_data = pd.read_csv(csv_file, encoding='utf-8')
        order_list = list()
        for _, row in pd_data.iterrows():
            fahuoshijian = row['发货时间']
            if len(fahuoshijian) <= 1:
                continue
            order = Order(
                shangpin=row['商品'],
                dingdanhao=row['订单号'],
                dingdanzhuangtai=row['订单状态'],
                shangpinshuliang=row['商品数量(件)'],
                shifoushenhezhong=row['是否审核中'],

                zhifushijian=cls.str2date(row['支付时间']),
                pindanchenggongshijian=cls.str2date(row['拼单成功时间']),
                dingdanquerenshijian=cls.str2date(row['订单确认时间']),
                chengnuofahuoshijian=cls.str2date(row['承诺发货时间']),
                fahuoshijian=cls.str2date(row['发货时间']),
                querenfahuoshijian=cls.str2date(row['确认收货时间']),

                shangpinid=row['商品id'],
                shangpinguige=row['商品规格'],
                yangshiid=row['样式ID'],
                skubianma=row['商家编码-SKU维度'],
                shangpinbianma=row['商家编码-商品维度'],

                shangjiabeizhu=row['商家备注'],
                maijialiuyan=row['买家留言'],
                shouhouzhuangtai=row['售后状态'],

                shangpinzongjia=row['商品总价(元)'],
                dianpuyouhuizhekou=row['店铺优惠折扣(元)'],
                pingtaiyouhuizhekou=row['平台优惠折扣(元)'],
                youfei=row['邮费(元)'],
                yonghushifujine=row['用户实付金额(元)'],
                shangjiashishoujine=row['商家实收金额(元)'],

                kuaididanhao=row['快递单号'],
                kuaidigongsi=row['快递公司'],
                sheng=row['省'],
                shi=row['市'],
                qu=row['区'],

                shifouchoujianghuoshiyong=row['是否抽奖或0元试用'],
            )
            order_list.append(order)
        return order_list

    @classmethod
    def add(cls, session, orders):
        return session.add_all(orders)

    @classmethod
    def query_fahuoshijian(cls, session, datetime_start, datetime_end):
        orders = session.query(Order).filter(Order.fahuoshijian >= datetime_start,
                                             Order.fahuoshijian <= datetime_end).all()
        return orders

    @classmethod
    def query_dingdanhao(cls, session, dingdanhao):
        return session.query(Order).filter(Order.dingdanhao == dingdanhao).first()


def test_dingdanruku():
    import os
    from lib.path import TEST_PATH

    order_files = os.path.join(TEST_PATH, 'orders.csv')
    orders = Order.csv2order(order_files)[1:3]
    if len(orders) <= 0:
        print('没有有效的订单')
        return -1

    # 剔除已经录入的订单
    # 找到时间范围
    datetime_start = datetime_end = orders[0].fahuoshijian

    for order in orders:
        fahuoshijian = order.fahuoshijian
        if fahuoshijian < datetime_start:
            datetime_start = fahuoshijian
        if fahuoshijian > datetime_end:
            datetime_end = fahuoshijian

    # 找到数据库里面这个时间范围的订单
    with session_scope() as session:
        orders_db = Order.query_fahuoshijian(session, datetime_start, datetime_end)

    # 剔除已经录入的订单
    dingdanhao_db = set()
    if orders_db is not None:
        for order in orders_db:
            dingdanhao_db.add(order.dingdanhao)

    orders_filter = list()
    for order in orders:
        if order.dingdanhao not in dingdanhao_db:
            orders_filter.append(order)

    print(f'有效订单数量：{len(orders_filter)}')

    # 将过滤以后的订单入库
    with session_scope() as session:
        result = Order.add(session, orders_filter)
        print(result)


def test_shangpin_ruku():
    shangpins = [
        {
            'bianma': '000101',
            'mingcheng': '春夏口罩-浅紫色',
            'danwei': '个',
        },
        {
            'bianma': '000102',
            'mingcheng': '春夏口罩-浅粉色',
            'danwei': '个',
        },
    ]
    with session_scope() as session:
        for shangpin in shangpins:
            result = Goods.add(session, shangpin)
            print(result)


def test_sku_ruku():
    skus = [
        {
            'bianma': '000101',
            'mingcheng': '【畅销装】紫红色+浅粉色',
            'danwei': '件',
            'goods_bianma': '000101',
            'shuliang': 1,
        },
        {
            'bianma': '000101',
            'mingcheng': '【畅销装】紫红色+浅粉色',
            'danwei': '件',
            'goods_bianma': '000102',
            'shuliang': 1,
        },
    ]

    with session_scope() as session:
        for sku in skus:
            result = Sku.add(session, sku)
            print(result)


def test_goods_get_to_dict():
    with session_scope() as session:
        result = Goods.query(session)
        print(result)
        print(result[0].to_dict())


if __name__ == '__main__':
    # test_dingdanruku()
    # test_shangpin_ruku()
    # test_sku_ruku()
    test_goods_get_to_dict()
