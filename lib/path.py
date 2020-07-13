#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-06-15 17:05
# @Author  : NingAnMe <ninganme@qq.com>
import os

dir_now = os.path.dirname(os.path.abspath(__file__))

ROOT_PATH = os.path.dirname(dir_now)
LIB_PATH = dir_now
AID_PATH = os.path.join(ROOT_PATH, 'aid')
TEST_PATH = os.path.join(ROOT_PATH, 'test')
