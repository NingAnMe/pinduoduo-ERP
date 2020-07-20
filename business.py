#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-04 19:47
# @Author  : NingAnMe <ninganme@qq.com>
import argparse
from dateutil.relativedelta import relativedelta

from lib.dbkucun import *
from lib.dbyingxiao import *
from lib.path import *
from json_data import str2json


def goods_add(goods_file):

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


def order_add(order_file):

    orders = Order.csv2order(order_file)
    if len(orders) <= 0:
        print('没有有效的订单')
        return -1

    # 剔除已经录入的订单
    # 找到时间范围
    datetime_start = datetime_end = orders[0]['fahuoshijian']

    for order in orders:
        fahuoshijian = order['fahuoshijian']
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
        if order["dingdanhao"] not in dingdanhao_db:
            orders_filter.append(order)

    print(f"有效订单数量：{len(orders_filter)}")

    # 将过滤以后的订单入库
    with session_scope() as session:
        Order.add(session, orders_filter)
        print("success")


def goods_qukucun(dingdanhao):
    with session_scope() as session:
        order = Order.query_dingdanhao(session, dingdanhao)
        if order is None or order.shifouqukucun == '是':
            return

        skubianma = order.skubianma
        skus = Sku.query_bianma(session, skubianma)
        if skus is None:
            return

        # 获取商品和商品需要减少的数量
        shangpinjianshu = order.shangpinshuliang
        for sku in skus:
            sku.sku_relate_goods.shuliang = sku.sku_relate_goods.shuliang - sku.shuliang * shangpinjianshu
        order.shifouqukucun = '是'
        return True


def goods_qukucun_datetime(dt_s, dt_e):
    dingdanhaos = list()
    with session_scope() as session:
        orders = Order.query_fahuoshijian(session, datetime_start=dt_s, datetime_end=dt_e)
        print(f'总订单数量：{len(orders)}')
        for order in orders:
            if order.shifouqukucun != '是':
                dingdanhaos.append(order.dingdanhao)
    print(f'本次有效订单数量：{len(dingdanhaos)}')
    count = 0
    for dingdanhao in dingdanhaos:
        r = goods_qukucun(dingdanhao)
        if r:
            count += 1
    print(f'本次处理订单数量： {count}')


def goods_detail_add(json_data, dt):
    datas = PddGoodsDetail.json2data(json_data)
    with session_scope() as session:
        result = PddGoodsDetail.query_datetime(session, dt)
        ids = set()
        for row in result:
            ids.add(row.goodsId)
        datas_filter = list()
        for i in datas:
            if str(i['goodsId']) not in ids:
                datas_filter.append(i)
        PddGoodsDetail.add(session, datas_filter)
        print(f"goods_detail_add Success: {len(datas_filter)}")


def ad_unit_add(json_data, dt, ad_type):
    datas = AdUnit.json2data(json_data, dt, ad_type)
    with session_scope() as session:
        result = AdUnit.query_datetime(session, dt)
        ids = set()
        for row in result:
            if row.adType == ad_type:
                ids.add(row.goodsId)
        datas_filter = list()
        for i in datas:
            if str(i['goodsId']) not in ids:
                datas_filter.append(i)
        AdUnit.add(session, datas_filter)
        print(f"ad_unit_add Success: {ad_type} 处理数据量：{len(datas_filter)}")


def deal_paid_free_order(dt):
    PddGoodsDetail.paid_free_order(dt)


def auto_business(dt_str):
    # str2json
    str2json()

    dt = datetime.strptime(dt_str, "%Y-%m-%d")
    # 订单入库
    order_file_path = kucun_order_path
    order_files = os.listdir(order_file_path)
    order_file = None
    for i in order_files:
        if ("orders_export" + dt_str) in i:
            order_file = os.path.join(order_file_path, i)
    if order_file is not None:
        print(f'订单文件：{order_file}')
        order_add(order_file)

    # 去库存
    dt_start = dt
    dt_end = dt + relativedelta(days=1) - relativedelta(minutes=1)
    goods_qukucun_datetime(dt_start, dt_end)

    # 添加营销数据
    json_data_files = os.listdir(json_file_path)
    for filename in json_data_files:
        if dt_str in filename:
            json_data_file = os.path.join(json_file_path, filename)
            # 添加商品销售数据
            if 'detail' in filename:
                goods_detail_add(json_data_file, dt)

            # 添加搜索推广数据
            if 'search' in filename:
                ad_unit_add(json_data_file, dt, 'search')

            # 添加场景推广数据
            if 'scene' in filename:
                ad_unit_add(json_data_file, dt, 'scene')

    # 处理付费订单和免费数量
    deal_paid_free_order(dt)


if __name__ == '__main__':
    # from lib.path import TEST_PATH
    # goods_file = os.path.join(TEST_PATH, 'goods.csv')
    # goods_add(goods_file)

    # order_file = os.path.join(TEST_PATH, 'orders.csv')
    # order_add(order_file)

    # goods_qukucun(['200705-660621394700582'])

    # ######################### 业务运行 ###################################
    parser = argparse.ArgumentParser(description='地外太阳能数据生产工具')
    parser.add_argument('--goodfile', '-g', help='商品信息文件')
    parser.add_argument('--orderfile', '-o', help='订单信息文件')
    parser.add_argument('--qukucun', '-q', help='去库存')
    parser.add_argument('--detail', help='去库存')
    parser.add_argument('--search', help='去库存')
    parser.add_argument('--scene', help='去库存')
    parser.add_argument('--datetime', help='开始时间(北京时)，YYYY-mm-dd(2019-01-01)')
    parser.add_argument('--datetime_start', '-s', help='开始时间(北京时)，YYYYmmddHHMM(201901010000)')
    parser.add_argument('--datetime_end', '-e', help='结束时间（北京时），YYYYmmddHHMM(201901010000)')
    parser.add_argument('--debug', help='是否DEBUG', default=False)
    parser.add_argument('--test', help='是否TEST', default=False)
    args = parser.parse_args()

    if args.goodfile is not None:
        print(f"<<< {args.goodfile}")
        goods_add(args.goodfile)

    if args.orderfile is not None:
        print(f"<<< {args.orderfile}")
        order_add(args.orderfile)

    if args.datetime_start is not None and args.datetime_end is not None:
        datetime_s = datetime.strptime(args.datetime_start, '%Y%m%d%H%M')
        datetime_e = datetime.strptime(args.datetime_end, '%Y%m%d%H%M')
        goods_qukucun_datetime(datetime_s, datetime_e)

    if args.datetime is not None:
        auto_business(args.datetime)
