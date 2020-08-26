#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-14 21:23
# @Author  : NingAnMe <ninganme@qq.com>
from collections import defaultdict
import json

import pandas as pd
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import relationship

from lib.database import *


class PddGoods(Base):
    __tablename__ = "pddGoods"

    # 商品编码
    id = Column(String, primary_key=True)
    outerId = Column(String, unique=True)
    shopId = Column(String)
    goodsId = Column(String)
    goodsTitle = Column(String)
    shortName = Column(String)
    categoryId = Column(String)
    categoryName = Column(String)
    saleStatus = Column(String)
    onlineStatus = Column(String)
    salePrice = Column(String)
    createTime = Column(DateTime)
    modifyTime = Column(DateTime)
    recordTime = Column(DateTime)

    imageUrl = Column(String)

    beizhu = Column(String)

    pddSku_rs = relationship("PddSku", backref="pddGoods_rs")
    pddGoodsDetail_rs = relationship("PddGoodsDetail", backref="pddGoods_rs")
    adUnit_rs = relationship("PddAdUnit", backref="pddGoods_rs")

    @classmethod
    def str2datetime(cls, date_str):
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except Exception as why:
            str(why)
            return None

    @classmethod
    def json2data(cls, json_file):
        with open(json_file, 'r') as fp:
            data_json = json.load(fp)
        if not data_json['success']:
            return

        key_list = {
            "id", "outerId", "shopId", "goodsId", "goodsTitle",
            "shortName", "categoryId", "categoryName", "saleStatus", "onlineStatus",
            "salePrice", "createTime", "modifyTime", "recordTime", "imageUrl",
        }
        goods_detail_list = list()
        for detail in data_json['data']:
            if detail['outerId'] is None or len(detail['outerId']) != 5:
                continue
            goods_detail = dict()
            for k, v in detail.items():
                if k in key_list:
                    goods_detail[k] = v
            if goods_detail['createTime'] is not None:
                goods_detail['createTime'] = cls.str2datetime(goods_detail['createTime'])
            if goods_detail['modifyTime'] is not None:
                goods_detail['modifyTime'] = cls.str2datetime(goods_detail['modifyTime'])
            if goods_detail['recordTime'] is not None:
                goods_detail['recordTime'] = cls.str2datetime(goods_detail['recordTime'])
            goods_detail_list.append(goods_detail)
        return goods_detail_list

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = PddGoods(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        if isinstance(data, list):
            session.bulk_insert_mappings(PddGoods, data)

    @classmethod
    def update(cls, session, data):
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            session.bulk_update_mappings(PddGoods, data)

    @classmethod
    def query(cls, session):
        return session.query(PddGoods).all()

    @classmethod
    def query_pdd_goods_outer_id(cls, session, outer_id):
        return session.query(PddGoods).filter(PddGoods.outerId == outer_id.strip()).all()


class PddSku(Base):
    __tablename__ = 'pddSku'

    id = Column(String)
    outerId = Column(String, primary_key=True)
    shopId = Column(String)
    goodsId = Column(String, ForeignKey('pddGoods.goodsId'))
    skuId = Column(String)
    skuName = Column(String)
    chengben = Column(Float, default=0)
    saleStatus = Column(String)
    onlineStatus = Column(String)
    shortName = Column(String)
    recordTime = Column(DateTime)
    imageUrl = Column(String)

    beizhu = Column(String)

    goods_rs = relationship("RelateSkuGoods", back_populates='pddSku_rs')  # goods关联的sku实例 = RelateSkuGoods.pddSku_rs

    @classmethod
    def str2datetime(cls, date_str):
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except Exception as why:
            str(why)
            return None

    @classmethod
    def json2data(cls, json_file):
        with open(json_file, 'r') as fp:
            data_json = json.load(fp)
        if not data_json['success']:
            return

        key_list = {
            "id", "outerId", "shopId", "goodsId", "skuId",
            "skuName", "saleStatus", "onlineStatus", "shortName", "recordTime",
            "imageUrl",
        }
        data_dict = dict()
        for detail in data_json['data']:
            if detail['outerId'] is None or len(detail['outerId']) != 5:
                continue
            for sku in detail['skuList']:
                sku_detail = dict()
                outer_id = sku['outerId']
                if outer_id is None or len(outer_id) != 7:
                    continue
                for k, v in sku.items():
                    if k in key_list:
                        sku_detail[k] = v
                sku_detail['recordTime'] = cls.str2datetime(sku_detail['recordTime'])
                if outer_id not in data_dict:
                    data_dict[outer_id] = sku_detail
                elif data_dict[outer_id]["recordTime"] < sku_detail["recordTime"]:
                    data_dict[outer_id] = sku_detail
        data_list = list(data_dict.values())
        return data_list

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = PddSku(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        if isinstance(data, list):
            session.bulk_insert_mappings(PddSku, data)

    @classmethod
    def update(cls, session, data):
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            session.bulk_update_mappings(PddSku, data)

    @classmethod
    def query(cls, session):
        return session.query(PddSku).all()

    @classmethod
    def query_pdd_sku_outer_id(cls, session, outer_id):
        return session.query(PddSku).filter(PddSku.outerId == outer_id.strip()).first()


class PddGoodsDetail(Base):
    __tablename__ = "pddGoodsDetail"

    id = Column(Integer, primary_key=True, autoincrement=True)
    statDate = Column(DateTime)  # 时间
    goodsId = Column(String, ForeignKey("pddGoods.goodsId"))

    goodsName = Column(String)  # 名称

    payOrdrGoodsQty = Column(Integer)  # 支付件数
    paidOrdrCnt = Column(Integer, default=0)  # 付费订单数量
    freeOrdrCnt = Column(Integer)  # 免费订单数量

    goodsUv = Column(Integer)  # 访客数
    goodsPv = Column(Integer)  # 浏览量
    goodsVcr = Column(Float)  # 转化率

    payOrdrAmt = Column(Float)  # 支付金额
    netProfit = Column(Float)  # 净利润

    payOrdrCnt = Column(Integer)  # 订单数量
    payOrdrUsrCnt = Column(Integer)  # 支付买家数
    cfmOrdrCnt = Column(Integer)  # 成团订单数
    cfmOrdrGoodsQty = Column(Integer)  # 成团件数
    goodsFavCnt = Column(Integer)  # 收藏数

    addOrcrCnt = Column(Integer)  # 补单订单数量
    addOrdrAmt = Column(Float)  # 补单金额

    @classmethod
    def str2datetime(cls, dt):
        return datetime.strptime(dt, '%Y-%m-%d')

    @classmethod
    def json2data(cls, json_file):
        with open(json_file, 'r') as fp:
            data_json = json.load(fp)
        if not data_json['success']:
            return

        key_list = {"goodsId", "statDate", "goodsName", "goodsFavCnt", "goodsUv", "goodsPv", "payOrdrCnt", "goodsVcr",
                    "payOrdrGoodsQty", "payOrdrUsrCnt", "payOrdrAmt", "cfmOrdrCnt", "cfmOrdrGoodsQty"}
        goods_detail_list = list()
        for detail in data_json['result']['goodsDetailList']:
            goods_detail = dict()
            for k, v in detail.items():
                if k in key_list:
                    goods_detail[k] = v
            goods_detail['statDate'] = cls.str2datetime(goods_detail['statDate'])
            goods_detail_list.append(goods_detail)
        return goods_detail_list

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = PddAdUnit(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        elif isinstance(data, list):
            session.bulk_insert_mappings(PddGoodsDetail, data)

    @classmethod
    def query_datetime(cls, session, dt):
        return session.query(PddGoodsDetail).filter(PddGoodsDetail.statDate == dt).all()

    @classmethod
    def paid_free_order(cls, dt):
        with session_scope() as session:
            result = PddAdUnit.query_datetime(session, dt)
            goods_paid_order = defaultdict(int)
            for row in result:
                goods_id = row.goodsId
                order_number = row.orderNum
                goods_paid_order[goods_id] += order_number
            result = PddGoodsDetail.query_datetime(session, dt)
            count = 0
            for row in result:
                if row.goodsId in goods_paid_order:
                    row.paidOrdrCnt = goods_paid_order[row.goodsId]
                row.freeOrdrCnt = row.payOrdrGoodsQty - row.paidOrdrCnt
                count += 1
            print('paid_free_order Success: 处理数据量：{count}'.format(count=count))


class PddAdUnit(Base):
    __tablename__ = 'pddAdUnit'

    id = Column(Integer, primary_key=True, autoincrement=True)
    statDate = Column(DateTime)  # 时间
    adType = Column(String)  # 广告类型，search, scene
    goodsId = Column(String, ForeignKey("pddGoods.id"))  # 商品id
    goodsName = Column(String)  # 名称
    impression = Column(Integer, default=0)  # 曝光量
    click = Column(Integer, default=0)  # 点击量
    ctr = Column(Float, default=0)  # 点击率
    spend = Column(Float, default=0)  # 花费
    roi = Column(Float, default=0)  # 投入产出比
    orderNum = Column(Integer, default=0)  # 订单量
    cpc = Column(Float, default=0)  # 平均点击花费
    cvr = Column(Float, default=0)  # 点击转化率
    cpm = Column(Float, default=0)  # 千次曝光花费
    mallFavNum = Column(Integer, default=0)  # 店铺关注量
    goodsFavNum = Column(Integer, default=0)  # 商品收藏量

    adId = Column(String)  # 广告ID

    @classmethod
    def str2datetime(cls, dt):
        return datetime.strptime(dt, '%Y-%m-%d')

    @classmethod
    def json2data(cls, json_file, dt, ad_type):
        if ad_type not in {"search", 'scene'}:
            return
        with open(json_file, 'r') as fp:
            data_json = json.load(fp)
        if not data_json['success']:
            return
        key_list = {"goodsId", "statDate", "goodsName", "impression", "click", "ctr", "spend", "roi",
                    "orderNum", "cpc", "cvr", "cpm", "mallFavNum", "goodsFavNum", "adId"}
        goods_detail_list = list()
        for detail in data_json['result']:
            goods_detail = dict()
            for k, v in detail.items():
                if k in key_list:
                    goods_detail[k] = v
            goods_detail['statDate'] = dt
            goods_detail['adType'] = ad_type
            if goods_detail['spend'] is not None:
                goods_detail['cpc'] /= 1000.
                goods_detail['cpm'] /= 1000.
                goods_detail['spend'] /= 1000.
            goods_detail_list.append(goods_detail)
        return goods_detail_list

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = PddAdUnit(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        elif isinstance(data, list):
            session.bulk_insert_mappings(PddAdUnit, data)

    @classmethod
    def query_datetime(cls, session, dt):
        return session.query(PddAdUnit).filter(PddAdUnit.statDate == dt).all()


class PddOrder(Base):
    __tablename__ = 'pddOrder'

    # 商品 订单号 订单状态  商品数量(件) 是否审核中
    dingdanhao = Column(String, primary_key=True)
    shangpin = Column(String)
    dingdanzhuangtai = Column(String)
    shangpinshuliang = Column(Integer)
    shifoushenhezhong = Column(String)

    # 支付时间 承诺发货时间 发货时间 确认收货时间
    zhifushijian = Column(DateTime)
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
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except Exception as why:
            str(why)
            return None

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
            order = dict(
                shangpin=row['商品'],
                dingdanhao=row['订单号'],
                dingdanzhuangtai=row['订单状态'],
                shangpinshuliang=row['商品数量(件)'],
                shifoushenhezhong=row['是否审核中'],

                zhifushijian=cls.str2date(row['支付时间']),
                chengnuofahuoshijian=cls.str2date(row['承诺发货时间']),
                fahuoshijian=cls.str2date(row['发货时间']),
                querenfahuoshijian=cls.str2date(row['确认收货时间']),

                shangpinid=row['商品id'],
                shangpinguige=row['商品规格'],
                yangshiid=row['样式ID'],
                skubianma=row['商家编码-SKU维度'].strip(),
                shangpinbianma=row['商家编码-商品维度'].strip(),

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
            if order['fahuoshijian'] is None:
                continue
            order_list.append(order)
        return order_list

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = PddOrder(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        elif isinstance(data, list):
            session.bulk_insert_mappings(PddOrder, data)

    @classmethod
    def query_fahuoshijian(cls, session, datetime_start, datetime_end):
        orders = session.query(PddOrder).filter(PddOrder.fahuoshijian >= datetime_start,
                                                PddOrder.fahuoshijian <= datetime_end).all()
        return orders

    @classmethod
    def query_dingdanhao(cls, session, dingdanhao):
        return session.query(PddOrder).filter(PddOrder.dingdanhao == dingdanhao).first()


class Goods(Base):
    __tablename__ = 'goods'

    # 商品编码
    bianma = Column(String, primary_key=True)
    mingcheng = Column(String)
    danwei = Column(String, default='个')
    kucun = Column(Integer, nullable=False, default=0)
    junjia = Column(Float, nullable=False, default=0)
    beizhu = Column(String)
    gengxinshijian = Column(DateTime)

    pddSku_rs = relationship("RelateSkuGoods", back_populates='goods_rs')
    goodsDetail_rs = relationship('GoodsDetail', backref='goods_rs')
    goodsOrder_rs = relationship('GoodsOrder', backref='goods_rs')

    def get_warning(self, session, days_mean: int, days_remain: int):
        """
        获取几天内的平均销量、库存足够的时间、是否警告
        :param days_remain:
        :param session:
        :param days_mean:
        :return:
        """
        goods_detail_result = GoodsDetail.query_order_by_datetime_bianma(
            session, self.bianma)[:days_mean]
        count = 1
        for goods_detail in goods_detail_result:
            count += goods_detail.xiaoliang
        xiaoliang_mean = count / days_mean
        remain_days = int(self.kucun / xiaoliang_mean)
        if remain_days < days_remain:
            warning = True
        else:
            warning = False
        return {
            'xiaoliang_mean': xiaoliang_mean,
            'remain_days': remain_days,
            'warning': warning,
        }

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = Goods(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        if isinstance(data, list):
            session.bulk_insert_mappings(Goods, data)

    @classmethod
    def delete(cls, session, data_id):
        session.query(Goods).filter(Goods.bianma == data_id).delete()

    @classmethod
    def update(cls, session, data):
        if isinstance(data, dict):
            bianma = data.pop('bianma')
            session.query(Goods).filter(Goods.bianma == bianma).update(data)
            session.flush()
            return session.query(Goods).filter(Goods.bianma == bianma).first().to_dict()
        if isinstance(data, list):
            session.bulk_update_mappings(Goods, data)

    @classmethod
    def query(cls, session):
        return session.query(Goods).all()

    @classmethod
    def query_bianma_in_(cls, session, bianmas):
        return session.query(Goods).filter(Goods.bianma.in_(bianmas)).all()

    @classmethod
    def goods_add_csv(cls, goods_file):
        """
        将商品信息导入数据库
        :param goods_file: csv文件
        :return:
        """
        goods_info = pd.read_csv(goods_file, index_col=None, encoding='GBK')
        for _, g in goods_info.iterrows():
            print(g.to_dict())

        with session_scope() as session:
            goods = Goods.query(session)
            bianmas = set()
            if goods is not None:
                for g in goods:
                    bianmas.add(g.bianma)

            for _, g in goods_info.iterrows():
                if str(g.bianma) not in bianmas:
                    g_dict = g.to_dict()
                    Goods.add(session, g_dict)


class RelateSkuGoods(Base):
    __tablename__ = 'relateSkuGoods'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sku_bianma = Column(String, ForeignKey('pddSku.outerId'))
    goods_bianma = Column(String, ForeignKey('goods.bianma'))
    shuliang = Column(Integer, nullable=False)
    pddSku_rs = relationship("PddSku", back_populates="goods_rs")
    goods_rs = relationship("Goods", back_populates="pddSku_rs")

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = RelateSkuGoods(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        if isinstance(data, list):
            session.bulk_insert_mappings(RelateSkuGoods, data)

    @classmethod
    def delete(cls, session, data_id):
        session.query(RelateSkuGoods).filter(RelateSkuGoods.id == data_id).delete()

    @classmethod
    def update(cls, session, data):
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            session.bulk_update_mappings(RelateSkuGoods, data)

    @classmethod
    def query(cls, session):
        return session.query(RelateSkuGoods).all()


class GoodsDetail(Base):
    __tablename__ = 'goodsDetail'
    id = Column(Integer, primary_key=True, autoincrement=True)
    statDate = Column(DateTime)  # 时间
    bianma = Column(String, ForeignKey("goods.bianma"))  # 商品编码
    xiaoliang = Column(Integer)  # 销量
    beizhu = Column(String)  # 备注

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = GoodsDetail(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        if isinstance(data, list):
            session.bulk_insert_mappings(GoodsDetail, data)

    @classmethod
    def query(cls, session):
        return session.query(GoodsDetail).all()

    @classmethod
    def query_order_by_datetime_bianma(cls, session, bianma):
        return session.query(GoodsDetail).filter(GoodsDetail.bianma == bianma)\
            .order_by(GoodsDetail.statDate.desc()).all()

    @classmethod
    def query_datetime(cls, session, dt_start, dt_end):
        return session.query(GoodsDetail).filter(
            GoodsDetail.statDate >= dt_start, GoodsDetail.statDate <= dt_end).all()

    @classmethod
    def query_datetime_bianma(cls, session, dt_start, dt_end, bianma):
        return session.query(GoodsDetail).filter(
            GoodsDetail.statDate >= dt_start, GoodsDetail.statDate <= dt_end).filter(GoodsDetail.bianma == bianma).all()

    @classmethod
    def delete_datetime_bianmas(cls, session, dt_start, dt_end, bianmas):
        return session.query(GoodsDetail).filter(
            GoodsDetail.statDate >= dt_start, GoodsDetail.statDate <= dt_end).filter(
            GoodsDetail.bianma.in_(bianmas)
        ).delete(synchronize_session=False)


class GoodsOrder(Base):
    """
    进货单
    """
    __tablename__ = 'goodsOrder'

    dingDanHao = Column(Integer, primary_key=True, autoincrement=True)  # 订单号
    bianMa = Column(String, ForeignKey("goods.bianma"), nullable=False)  # 商品编码
    danWei = Column(String, default='个')  # 单位
    shuLiang = Column(Integer, nullable=False)  # 数量
    shangPinJiaZhi = Column(Float, nullable=False)  # 商品价值
    yunFei = Column(Float, default=0)  # 运输费用
    zongJia = Column(Float, nullable=False)  # 总价 = 商品价值 + 运费费用，自动计算
    danJia = Column(Float, nullable=False)  # 单价 = 总价 / 数量，自动计算
    jinHuoDate = Column(DateTime, nullable=False)  # 进货时间
    beiZhu = Column(String)  # 备注

    @classmethod
    def add(cls, session, data):
        if isinstance(data, dict):
            data = GoodsOrder(**data)
            session.add(data)
            session.flush()
            return data.to_dict()
        if isinstance(data, list):
            session.bulk_insert_mappings(GoodsOrder, data)
            return data

    @classmethod
    def query(cls, session):
        return session.query(GoodsOrder).all()

    @classmethod
    def query_dingdanhao(cls, session, dingdanhao):
        return session.query(GoodsOrder).filter(GoodsOrder.dingDanHao == dingdanhao).first()

    @classmethod
    def query_datetime(cls, session, dt_start, dt_end):
        return session.query(GoodsOrder).filter(
            GoodsOrder.jinHuoDate >= dt_start, GoodsOrder.jinHuoDate <= dt_end).all()

    @classmethod
    def delete(cls, session, dingdanhao):
        session.query(GoodsOrder).filter(GoodsOrder.dingDanHao == dingdanhao).delete()
        return {
            "dingDanHao": dingdanhao,
            "message": "成功删除"
        }


def test_paid_free_order():
    dt = PddGoodsDetail.str2datetime("2020-07-17")
    PddGoodsDetail.paid_free_order(dt)


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    # test_PddGoods()
    # test_PddSku()
