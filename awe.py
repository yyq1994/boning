# coding：<encoding name> ： # coding: utf-8
'''
bnse.boninsge.com

检查今天在两小时内的数据，并把检查信息保存在桌面bnse3.0_xlsx中
返回文件样式，如下：
建筑_编码	建筑名称	是否有计算值	是否有原始值	是否正常	备注
3709021001	泰安爱琴海	有		正常
3716022201	滨州市爱琴海购物公园	有		正常
3714262201	东海天下康养小镇	有		正常

大体逻辑
1. servicedata 支路有没有值，
2. 没有查找meterdata 处理后的原始值，有原始值则输出当前计算时间
3. 没有查找10.11 original有没有原始值
'''


import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import time
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)


class CheckData(object):
    def __init__(self):
        # self.server = ['aiqinhai','binzhou','dezhou','dongying','hospital','jimo',
        #                'jimo2','jimo3','jinan','liaocheng','qianyi','taian','weifang']
        self.server = ['aiqinhai']
        self.handle_server(self.server)

    @staticmethod
    def sqlalchemy_connect():
        engine = create_engine(f'mysql+pymysql://bnse:123456@192.168.10.12:3306/base?charset=utf8')
        return engine

    #找到每个数据库下的所有建筑
    def handle_server(self,server):
        for i in server:
            sql = f'select sign,name from bnse_ems_{i}.building'
            # print(sql)
            conn = self.sqlalchemy_connect()

            df_building = pd.read_sql(sql, conn)
            self.handle_building(df_building,conn, i)
            conn.dispose()
            exit(1)

    #每个建筑下的所有支路是否有数，
    # 没有支路值，检查原始值，没有原始值输出没有原始值，有原始值输出当前计算时间
    def handle_building(self,buildId,conn,server):
        print(buildId)

        # buildId = pd.DataFrame([['3703030034','黄金一号总部大厦']],columns=['sign','name'])
        # print(buildId)
        date = datetime.now()
        for index,row in buildId.iterrows():
            print('当前建筑： ',row["name"],row["sign"])

            sql = f'select sign from `bnse_ems_{server}`.building where sign = {row["sign"]}'
            df_meters = pd.read_sql(sql, conn)['sign'][0]
            print(df_meters)
            conn.dispose()
            exit(1)
            # df_meters = pd.read_sql(sql, conn).T
            # print(df_topclass)
            tuples = [tuple(x) for x in df_topclass.values][0]
            if len((tuples)) == 0:
                print('没有MerterId')
                # continue
                sql_service_id = f'SELECT ServiceId FROM `bnse_ems`.`servicecascade` where ParentServiceId is null'
                sql_meter_id = f'SELECT MeterId FROM `bnse_ems`.`servicecascade` where ParentServiceId in ({sql_service_id})'
                sql = f'SELECT sign from `bnse_ems`.`meter` where BuildingId = {df_buildingid} and id in ({sql_meter_id})'
                print(sql)
                conn.dispose()
                exit(1)


                df_topclass = pd.read_sql(sql, conn).T
                print(df_topclass)
                tuples = [tuple(x) for x in df_topclass.values][0]
                print(tuples)
                # conn.dispose()
                # exit(1)


            print(list(tuples))
            # conn.dispose()
            # exit(1)
            try:
                sql = f'select sum(data) from `bnse_servicedata`.`{date.year}{date.month:02d}_servicedata_{row["sign"]}_t1` ' \
                      f'where timefrom >= "{date.date()} {date.hour-15:2d}" and  sign in {tuples}'

                print(sql)
                # conn.dispose()
                # exit(1)
                df = pd.read_sql(sql, conn)
                print(df)

                conn.dispose()
                exit(1)
            except Exception as e:
                print(e)
                conn.dispose()
                exit(1)
            # self.handle_meter(df,row["sign"],date)

if __name__ == '__main__':
    CheckData()

