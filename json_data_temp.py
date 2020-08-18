#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-19 22:32
# @Author  : NingAnMe <ninganme@qq.com>
import os
import json
from lib.config import json_file_path

mail_id = ""

json_datas = {

    "detail": '',

    "search": '',

    "scene": '',

    "pddGoods": '',

}


def str2json():

    dt_json = json.loads(json_datas['detail'])['result']['goodsDetailList'][0]['statDate']
    mail_json = json.loads(json_datas['search'])['result'][0]["mallId"]

    for k, v in json_datas.items():
        filename = f'{mail_json}{k}{dt_json}.json'
        out_file = os.path.join(json_file_path, filename)
        if os.path.isfile(out_file):
            continue
        with open(out_file, 'w') as fp:
            json_data = json.loads(v)
            json.dump(json_data, fp)
            print(f"str2json Success: {out_file}")


if __name__ == '__main__':
    str2json()
