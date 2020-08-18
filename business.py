#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-04 19:47
# @Author  : NingAnMe <ninganme@qq.com>
import argparse
from dateutil.relativedelta import relativedelta

from lib.config import *
from lib.dbkucun import *
from lib.dbyingxiao import *
from lib.path import *


def order_add(order_file, dt_s, dt_e):
    """
    将订单文件导入数据库
    :param order_file: csv文件
    :param dt_s: 发货时间-开始时间
    :param dt_e: 发货时间-结束时间
    :return:
    """
    orders = PddOrder.csv2order(order_file)
    if len(orders) <= 0:
        print('没有有效的订单')
        return -1
    orders_filter_by_datetime = list()
    for order in orders:
        if dt_s <= order['fahuoshijian'] <= dt_e:
            orders_filter_by_datetime.append(order)

    if len(orders_filter_by_datetime) <= 0:
        print(f'没有有效的订单：{dt_s} to {dt_e}')
        return -1

    # 剔除已经录入的订单
    # 找到数据库里面这个时间范围的订单
    with session_scope() as session:
        orders_db = PddOrder.query_fahuoshijian(session, dt_s, dt_e)

        # 剔除已经录入的订单
        dingdanhao_db = set()
        if orders_db is not None:
            for order in orders_db:
                dingdanhao_db.add(order.dingdanhao)

    orders_filter = list()
    for order in orders_filter_by_datetime:
        if order["dingdanhao"] not in dingdanhao_db:
            orders_filter.append(order)

    print(f"有效订单数量：{len(orders_filter)}")

    # 将过滤以后的订单入库
    with session_scope() as session:
        PddOrder.add(session, orders_filter)
        print("success")


def goods_qukucun(dingdanhao):
    with session_scope() as session:
        order = PddOrder.query_dingdanhao(session, dingdanhao)
        if order is None or order.shifouqukucun == '是':
            return

        skubianma = order.skubianma
        sku = PddSku.query_bianma(session, skubianma)
        if sku is None:
            print(f"没有对应的sku：{skubianma}")
            return

        # 获取商品和商品需要减少的数量
        jianshu = order.shangpinshuliang
        for goods in sku.goods:
            goods.goods.shuliang -= goods.shuliang * jianshu
        order.shifouqukucun = '是'
        return True


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
    datas = PddAdUnit.json2data(json_data, dt, ad_type)
    with session_scope() as session:
        result = PddAdUnit.query_datetime(session, dt)
        ids = set()
        for row in result:
            if row.adType == ad_type:
                ids.add(row.adId)
        datas_filter = list()
        for i in datas:
            if str(i['adId']) not in ids:
                datas_filter.append(i)
        PddAdUnit.add(session, datas_filter)
        print(f"ad_unit_add Success: {ad_type} 处理数据量：{len(datas_filter)}")


def deal_paid_free_order(dt):
    PddGoodsDetail.paid_free_order(dt)


# def goods_detail_add(dt_s, dt_e):
#     if dt_s == dt_e:
#         dt_e = dt_e + relativedelta(days=1) - relativedelta(minutes=1)
#
#     count = 0
#     with session_scope() as session:
#         orders = PddOrder.query_fahuoshijian(session, datetime_start=dt_s, datetime_end=dt_e)
#         print(f'本次有效订单数量：{len(orders)}')
#
#         goods_xiaoliang = defaultdict(int)
#         for order in orders:
#             skubianma = order.skubianma
#             sku = PddSku.query_bianma(session, skubianma)
#             if sku is None:
#                 print(f"没有对应的sku：{skubianma}")
#                 continue
#
#             # 获取商品和商品需要减少的数量
#             jianshu = order.shangpinshuliang
#             for goods in sku.goods:
#                 goods_xiaoliang[goods.goods.bianma] += goods.shuliang * jianshu
#             count += 1
#             print(f'本次处理订单数量： {count}')
#         goods_details = list()
#         for k, v in goods_xiaoliang.items():
#             goods_details.append({
#                 'statDate': dt_s,
#                 'bianma': k,
#                 'xiaoliang': v,
#             })
#         GoodsDetail.add(session, goods_details)


def goods_qukucun_datetime(dt_s, dt_e):
    if dt_s == dt_e:
        dt_e = dt_e + relativedelta(days=1) - relativedelta(minutes=1)
    dingdanhaos = list()
    with session_scope() as session:
        orders = PddOrder.query_fahuoshijian(session, datetime_start=dt_s, datetime_end=dt_e)
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


def order_file_ruku_datetime(dt_s, dt_e):
    print(dt_s, dt_e)
    order_files = os.listdir(order_file_path)
    order_files.sort()
    while dt_s <= dt_e:
        dt_tmp_s = dt_s
        dt_tmp_e = dt_tmp_s + relativedelta(days=1) - relativedelta(seconds=1)
        dt_str = dt_tmp_s.strftime('%Y-%m-%d')
        # 处理订单并入库
        order_file = None
        for filename in order_files:
            if ("orders_export" + dt_str) in filename:
                order_file = os.path.join(order_file_path, filename)
        if order_file is not None:
            print(f'订单文件：{order_file}')
            order_add(order_file, dt_tmp_s, dt_tmp_e)
        dt_s += relativedelta(days=1)


def detail_file_ruku_datetime(dt_s, dt_e):
    json_data_files = os.listdir(json_file_path)
    json_data_files.sort()

    while dt_s <= dt_e:
        dt_tmp = dt_s
        dt_str = dt_tmp.strftime('%Y-%m-%d')
        dt = datetime.strptime(dt_str, "%Y-%m-%d")

        # 添加营销数据
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
        dt_s += relativedelta(days=1)


def set_all_order_qu_ku_cun_yes(dt_s, dt_e):
    # 将所有订单的状态设置为去库存
    with session_scope() as session:
        orders = session.query(PddOrder).filter(PddOrder.fahuoshijian >= dt_s,
                                                PddOrder.fahuoshijian <= dt_e,
                                                PddOrder.shifouqukucun != '是').all()
        for order in orders:
            order.shifouqukucun = '是'
        print(f"set_all_order_qu_ku_cun Success: 处理数据量：{len(orders)}")


def set_all_order_qu_ku_cun_no(dt_s, dt_e):
    # 将所有订单的状态设置为去库存
    with session_scope() as session:
        orders = session.query(PddOrder).filter(PddOrder.fahuoshijian >= dt_s,
                                                PddOrder.fahuoshijian <= dt_e,
                                                PddOrder.shifouqukucun != '否').all()
        for order in orders:
            order.shifouqukucun = '否'
        print(f"set_all_order_qu_ku_cun Success: 处理数据量：{len(orders)}")


if __name__ == '__main__':
    # ######################### 业务运行 ###################################
    parser = argparse.ArgumentParser(description='地外太阳能数据生产工具')
    parser.add_argument('--addOrder', help='处理订单文件，数据入库')
    parser.add_argument('--quKuCun', help='销库存')
    parser.add_argument('--addGoodsDetail', help='处理营销Json文件，数据入库')
    parser.add_argument('--date', help='开始时间(北京时)，YYYY-mm-dd(2019-01-01)')
    parser.add_argument('--dateStart', '-s', help='开始时间(北京时)，YYYY-mm-dd(2019-01-01)')
    parser.add_argument('--dateEnd', '-e', help='结束时间（北京时），YYYY-mm-dd(2019-01-01)')
    parser.add_argument('--setAllOrderQuKuCunYes', help='将所有的订单状态设置为去库存', default=False)
    parser.add_argument('--setAllOrderQuKuCunNo', help='将所有的订单状态设置为去库存', default=False)
    parser.add_argument('--debug', help='是否DEBUG', default=False)
    parser.add_argument('--test', help='是否TEST', default=False)
    args = parser.parse_args()

    datetime_ = None
    if args.date is not None:
        datetime_ = datetime.strptime(args.date, '%Y-%m-%d')

    if args.date is not None and args.dateStart is None:
        args.dateStart = args.date

    datetime_s = datetime_e = None
    if args.dateStart is not None:
        datetime_s = datetime.strptime(args.dateStart, '%Y-%m-%d')
        if args.dateEnd is not None:
            datetime_e = datetime.strptime(args.dateEnd, '%Y-%m-%d')
        else:
            datetime_e = datetime_s

    # 处理订单文件
    if args.addOrder:
        print(f'addOrder: {args.addOrder}')
        order_file_ruku_datetime(datetime_s, datetime_e)

    # 销库存
    if args.quKuCun:
        goods_qukucun_datetime(datetime_s, datetime_e)

    # 营销数据入库且处理
    if args.addGoodsDetail:
        detail_file_ruku_datetime(datetime_s, datetime_e)

    # 将所有订单的状态设置为已经去库存的状态'是'
    if args.setAllOrderQuKuCunYes:
        set_all_order_qu_ku_cun_yes(datetime_s, datetime_e)

    # 将所有订单的状态设置为已经去库存的状态’否‘
    if args.setAllOrderQuKuCunNo:
        set_all_order_qu_ku_cun_no(datetime_s, datetime_e)

    # 销量统计
    goods_xiaoliang_tongji(datetime_s, datetime_e)
