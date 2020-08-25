#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-04 23:05
# @Author  : NingAnMe <ninganme@qq.com>

"""
接口跨域：https://my.oschina.net/ykbj/blog/2086068
SQLAlchemy查询对象转字典：https://www.cnblogs.com/sanduzxcvbnm/p/10220718.html
JSON数据验证：https://pydantic-docs.helpmanual.io/
"""
from dateutil.relativedelta import relativedelta
from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
from werkzeug.datastructures import FileStorage
from lib.dbyingxiao import *

app = Flask(__name__)
CORS(app)
api = Api(app)

std_error = {
    'content': None,
    'message': '请求失败',
    'code': 1000
}

std_success = {
    'content': None,
    'message': '请求成功',
    'code': 0
}


def get_std_error(content=None, message='请求失败', code=1000):
    return {
        'content': content,
        'message': message,
        'code': code
    }


def get_std_success(content=None, message='请求成功', code=0):
    return {
        'content': content,
        'message': message,
        'code': code
    }


class PddGoodsApi(Resource):

    def get(self):
        with session_scope() as session:
            pdd_goods_outer_id = request.args.get('pddGoodsOuterId')
            if pdd_goods_outer_id is not None:
                results = PddGoods.query_pdd_goods_outer_id(session, pdd_goods_outer_id)
            else:
                results = PddGoods.query(session)
            if results is not None:
                content = list()
                for row in results:
                    pdd_goods_data = row.to_dict()
                    content.append(pdd_goods_data)
                content = sorted(content, key=lambda x: x['outerId'])
                return get_std_success(content)
            else:
                return get_std_error("没有找到商品数据")

    def post(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                PddGoods.add(session, data=data)
                return std_success
        else:
            return std_error

    def put(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                PddGoods.update(session, data=data)
                return std_success
        else:
            return std_error


class PddSkuApi(Resource):

    def get(self):
        with session_scope() as session:
            pdd_goods_outer_id = request.args.get('pddGoodsOuterId')
            pdd_sku_outer_id = request.args.get('pddSkuOuterId')
            if pdd_goods_outer_id:
                results = PddGoods.query_pdd_goods_outer_id(session, pdd_goods_outer_id)
                if results is not None:
                    results = results[0].pddSku_rs
            elif pdd_sku_outer_id:
                results = PddSku.query_pdd_sku_outer_id(session, pdd_sku_outer_id)
            else:
                results = PddSku.query(session)
            if results is not None:
                content = [i.to_dict() for i in results]
                content = sorted(content, key=lambda x: x['outerId'])
                return {
                    'content': content,
                    'message': '请求成功',
                    'code': 0
                }
        return std_error

    def post(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                PddSku.add(session, data)
                return std_success
        else:
            return std_error

    def put(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                PddSku.update(session, data)
                return std_success
        else:
            return std_error


class PddOrderApi(Resource):
    def post(self):
        """
        将订单文件导入数据库
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('orderFile', type=FileStorage, location='files')
        order_file = parser.parse_args().get('orderFile')
        orders = PddOrder.csv2order(order_file)
        if len(orders) <= 0:
            print('没有有效的订单')
            return std_error
        else:
            print(f'订单总量：{len(orders)}')

        # 获取最小发货时间和最大发货时间
        dt_s = orders[0]["fahuoshijian"]
        dt_e = orders[0]["fahuoshijian"]
        for order in orders:
            dt_order = order['fahuoshijian']
            if dt_order < dt_s:
                dt_s = dt_order
            elif dt_order > dt_e:
                dt_e = dt_order

        # 过滤已经入库的订单
        # 找到数据库里面这个时间范围的订单
        with session_scope() as session:
            orders_db = PddOrder.query_fahuoshijian(session, dt_s, dt_e)

            # 剔除已经录入的订单
            dingdanhao_db = set()
            if orders_db is not None:
                for order in orders_db:
                    dingdanhao_db.add(order.dingdanhao)
            print(f'时间段内数据库中存在的数据总量：{len(dingdanhao_db)}')

        orders_filter = list()
        for order in orders:
            if order["dingdanhao"] not in dingdanhao_db:
                orders_filter.append(order)

        print(f"需要入库的订单数量：{len(orders_filter)}")

        # 将过滤以后的订单入库
        with session_scope() as session:
            PddOrder.add(session, orders_filter)
            return std_success


class GoodsApi(Resource):

    def get(self):
        days_mean = 4  # 计算最近几天的平均销量
        days_remain = 7  # 库存不够7天使用的时候，进行预警
        args = request.args
        with session_scope() as session:
            pdd_sku_outer_id = args.get('pddSkuOuterId')
            if pdd_sku_outer_id:
                content = list()
                results = PddSku.query_pdd_sku_outer_id(session, pdd_sku_outer_id)
                if results is not None:
                    for rs_sku_goods in results.goods_rs:
                        data = rs_sku_goods.goods_rs.to_dict()
                        data['shuliang'] = rs_sku_goods.shuliang
                        data['sku_bianma'] = rs_sku_goods.sku_bianma
                        data['goods_bianma'] = rs_sku_goods.goods_bianma
                        data['id'] = rs_sku_goods.id
                        warning_info = rs_sku_goods.goods_rs.get_warning(session, days_mean, days_remain)
                        data = dict(data, **warning_info)
                        content.append(data)
            else:
                results = Goods.query(session)
                content = list()
                if results is not None:
                    for goods in results:
                        data = goods.to_dict()
                        warning_info = goods.get_warning(session, days_mean, days_remain)
                        data = dict(data, **warning_info)
                        content.append(data)
            content = sorted(content, key=lambda x: x['bianma'])
            return {
                'content': content,
                'message': '请求成功',
                'code': 0
            }

    def post(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                data_result = Goods.add(session, data=data)
                content = data_result
                return {
                    'content': content,
                    'message': '请求成功',
                    'code': 0
                }
        else:
            return std_error

    def put(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                Goods.update(session, data=data)
                return std_success
        else:
            return std_error

    def delete(self):
        data = request.args
        if data is not None and 'bianma' in data:
            with session_scope() as session:
                data_id = data['bianma']
                Goods.delete(session, data_id)
                return std_success
        else:
            return std_error


class GoodsDetailApi(Resource):
    def post(self):
        data = request.json
        dt_start = datetime.strptime(data['datetimeStart'], "%Y-%m-%d")
        dt_end = datetime.strptime(data['datetimeEnd'] + ' 23:59:59', "%Y-%m-%d %H:%M:%S")
        while dt_start <= dt_end:
            dt_today_s = dt_start
            dt_today_e = dt_start + relativedelta(days=1) - relativedelta(seconds=1)
            with session_scope() as session:
                sku_count = defaultdict(int)
                result = PddOrder.query_fahuoshijian(session, dt_today_s, dt_today_e)
                print(f'{dt_start} 订单数量：{len(result)}')
                if not result:
                    return
                for order in result:
                    if not order.skubianma:
                        continue
                    sku_count[order.skubianma] += order.shangpinshuliang
                goods_count = defaultdict(int)
                for sku_bianma, count in sku_count.items():
                    sku = PddSku.query_pdd_sku_outer_id(session, sku_bianma)
                    if not sku:
                        continue
                    for rs_sku_pdd in sku.goods_rs:
                        goods_count[rs_sku_pdd.goods_bianma] += rs_sku_pdd.shuliang * count
                goods_bianmas = goods_count.keys()
                GoodsDetail.delete_datetime_bianmas(session, dt_today_s, dt_today_e, goods_bianmas)
                goods_detail_list = list()
                for goods_bianma, xiaoliang in goods_count.items():
                    goods_detail_list.append({
                        'statDate': dt_start,
                        'bianma': goods_bianma,
                        'xiaoliang': xiaoliang,
                    })
                GoodsDetail.add(session, goods_detail_list)
                dt_start += relativedelta(days=1)
        return std_success


class GoodsGengXinShiJian(Resource):
    def put(self):
        data = request.args
        dt = datetime.strptime(data['datetime'] + ' 23:59:59', "%Y-%m-%d %H:%M:%S")
        with session_scope() as session:
            goods_result = Goods.query(session)
            goods_list = list()
            for goods in goods_result:
                goods_list.append({
                    'bianma': goods.bianma,
                    'gengxinshijian': dt
                })
            Goods.update(session, goods_list)
            return std_success


class GoodsKuCunApi(Resource):
    def put(self):
        data = request.json
        dt_start = datetime.strptime(data['datetimeStart'], "%Y-%m-%d")
        dt_end = datetime.strptime(data['datetimeEnd'] + ' 23:59:59', "%Y-%m-%d %H:%M:%S")
        if dt_end > datetime.now():
            dt_end = datetime.now()
        with session_scope() as session:
            goods_result = Goods.query(session)
            kucun_list = list()
            for goods in goods_result:
                dt_start_tmp = max(goods.gengxinshijian, dt_start)  # 不重复处理更新时间之前的订单
                dt_end_tmp = datetime(1970, 1, 1)
                goods_detail_result = GoodsDetail.query_datetime_bianma(session, dt_start_tmp, dt_end, goods.bianma)
                goods_info = {
                    'bianma': goods.bianma,
                }
                xiaoliang = 0
                for goods_detail in goods_detail_result:
                    xiaoliang += goods_detail.xiaoliang
                    dt_end_tmp = max(dt_end_tmp, goods_detail.statDate)  # 将更新时间修改为最新的销量统计时间
                goods_info["kucun"] = goods.kucun - xiaoliang
                # goods_info["kucun"] = goods.kucun + xiaoliang  # 恢复库存用
                goods_info["gengxinshijian"] = max(goods.gengxinshijian, dt_end_tmp)
                kucun_list.append(goods_info)
            if kucun_list:
                Goods.update(session, kucun_list)
                return std_success
            else:
                return std_error


class RelateSkuGoodsApi(Resource):

    def post(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                if 'id' in data:
                    data.pop('id')
                data_result = RelateSkuGoods.add(session, data=data)
                content = data_result
                return {
                    'content': content,
                    'message': '请求成功',
                    'code': 0
                }
        else:
            return std_error

    def put(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                RelateSkuGoods.update(session, data=data)
                return std_success
        else:
            return std_error

    def delete(self):
        data = request.args
        if data is not None and 'id' in data:
            with session_scope() as session:
                data_id = data['id']
                RelateSkuGoods.delete(session, data_id)
                return std_success
        else:
            return std_error


class GoodsOrderApi(Resource):

    def get(self):
        data = request.args
        dt_start = data.get('datetimeStart')
        dt_end = data.get('datetimeEnd')
        if dt_start is None and dt_end is None:
            dt_start = datetime.strptime(dt_start, "%Y-%m-%d")
            dt_end = datetime.strptime(dt_end, "%Y-%m-%d")
            with session_scope() as session:
                result = GoodsOrder.query_datetime(session, dt_start, dt_end)
                content = list()
                for row in result:
                    content.append(row.to_dict())
                return get_std_success(content)
        else:
            with session_scope() as session:
                result = GoodsOrder.query(session)
                content = list()
                for row in result:
                    content.append(row.to_dict())
                return get_std_success(content)

    def post(self):
        data = request.json
        if data is not None:
            with session_scope() as session:
                if 'dingDanHao' in data:
                    data.pop('dingDanHao')
                # 进货单入库
                data["zongJia"] = float(data['shangPinJiaZhi']) + float(data.get('yunFei', 0))
                data["danJia"] = data["zongJia"] / int(data['shuLiang'])
                data["jinHuoDate"] = datetime.strptime(data["jinHuoDate"], "%Y-%m-%d")
                goods_order_add = GoodsOrder.add(session, data=data)
                # 修改商品的库存
                bianmas = [data['bianMa']]
                goods_query = Goods.query_bianma_in_(session, bianmas)
                goods = goods_query[0]
                goods_dict = goods.to_dict()
                goods_dict['kucun'] = goods.kucun + int(data['shuLiang'])
                goods_dict.pop('gengxinshijian')
                goods_update = Goods.update(session, goods_dict)

                content = {
                    'goodsOrder': goods_order_add,
                    'goods': goods_update,
                }
                return get_std_success(content)
        else:
            return get_std_error("没有成功入库")

    def delete(self):
        data = request.args
        if data is not None:
            with session_scope() as session:
                if 'dingDanHao' in data:
                    dingdanhao = int(data['dingDanHao'])
                    # 修改商品的库存
                    goods_order_query = GoodsOrder.query_dingdanhao(session, dingdanhao)
                    goods_order_dict = goods_order_query.to_dict()

                    bianmas = [goods_order_dict['bianMa']]
                    goods_result = Goods.query_bianma_in_(session, bianmas)
                    goods = goods_result[0]
                    goods_dict = goods.to_dict()
                    goods_dict['kucun'] = goods.kucun - goods_order_dict["shuLiang"]
                    goods_dict.pop('gengxinshijian')
                    goods_result = Goods.update(session, goods_dict)
                    # 删除入库单
                    GoodsOrder.delete(session, dingdanhao)
                    content = {
                        'goodsOrder': goods_order_dict,
                        'goods': goods_result,
                    }
                    return get_std_success(content)
                else:
                    return get_std_error("没有找到订单号参数:dingDanHao")
        else:
            return get_std_error("没有获取到请求参数")


api.add_resource(PddGoodsApi, '/erp/pddGoods')
api.add_resource(PddSkuApi, '/erp/pddSku')
api.add_resource(PddOrderApi, '/erp/pddOrder')
api.add_resource(GoodsApi, '/erp/goods')
api.add_resource(GoodsDetailApi, '/erp/goodsDetail')
api.add_resource(GoodsOrderApi, '/erp/goodsOrder')
api.add_resource(GoodsGengXinShiJian, '/erp/goodsGengXinShiJian')
api.add_resource(GoodsKuCunApi, '/erp/goodsKuCun')
api.add_resource(RelateSkuGoodsApi, '/erp/relateSkuGoods')

if __name__ == '__main__':
    # host = '192.168.124.21'
    host = '192.168.43.213'
    # host = '192.168.8.102'
    # host = '192.168.0.170'
    app.run(debug=True, host=host, port=5000)
