#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-04 23:05
# @Author  : NingAnMe <ninganme@qq.com>

"""
接口跨域：https://my.oschina.net/ykbj/blog/2086068
SQLAlchemy查询对象转字典：https://www.cnblogs.com/sanduzxcvbnm/p/10220718.html
"""
from flask import Flask, request
from flask_restful import Resource, Api
from flask_cors import CORS
from lib.dbkucun import *

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


class GoodsApi(Resource):
    def get(self):
        with session_scope() as session:
            results = Goods.query(session)
            if results is not None:
                content = [i.to_dict() for i in results]
                for i in content:
                    i['gengxinshijian'] = i['gengxinshijian'].strftime('%Y-%m-%d %H:%M:%S')
                return {
                    'content': content,
                    'message': '请求成功',
                    'code': 0
                }
        return std_error


class SkuApi(Resource):

    def get(self):
        with session_scope() as session:
            results = Sku.query(session)
            content = [i.to_dict() for i in results]
            for i in content:
                i['gengxinshijian'] = i['gengxinshijian'].strftime('%Y-%m-%d %H:%M:%S')
            if results is not None:
                content = [i.to_dict() for i in results]
                for i in content:
                    i['gengxinshijian'] = i['gengxinshijian'].strftime('%Y-%m-%d %H:%M:%S')
                return {
                    'content': content,
                    'message': '请求成功',
                    'code': 0
                }
        return std_error

    def post(self):
        requests = request.json
        if requests is not None:
            with session_scope() as session:
                for sku in requests:
                    Sku.add(session, sku)
                return std_success
        return std_error


api.add_resource(GoodsApi, '/pdd/goods')
api.add_resource(SkuApi, '/pdd/sku')


if __name__ == '__main__':
    host = '192.168.124.21'
    app.run(debug=True, host=host, port=5000)
