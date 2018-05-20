import contextlib

import datetime
import sqlite3

import os
import pandas as pd
import numpy as np
import itertools


class WareIndex(object):

    # 管理商品信息，按计算过程中出现的顺序来给商品编号(index)
    # 商品次序号就是矩阵中商品的位置

    def __init__(self, db_path, ware_info_path):
        self.db_path = db_path
        self.ware_index = pd.DataFrame()
        self.ware_info = pd.DataFrame()
        if ware_info_path is None:
            self.simulate_ware_info()
        else:
            self.read_ware_info(ware_info_path)

    def add_wares(self, order_ware):

        sku_occur = (order_ware.groupby('ware_id', as_index=False).agg({'order_time': 'max'})
                     .merge(self.ware_info[['ware_id', 'cat_1st', 'cat_2nd', 'cat_3rd', 'brand']],
                            how='left', on='ware_id')
                     .rename(columns={'order_time': 'first_occur_time'})
                     )
        sku_occur['cal_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")

        with contextlib.closing(sqlite3.connect(self.db_path)) as wareIndexDb:
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", wareIndexDb)
            if 'ware_index' in list(tables['name']):
                self.ware_index = pd.read_sql("SELECT * FROM ware_index", wareIndexDb)
                self.ware_index = (self.ware_index
                                   .drop(['index'], axis=1)
                                   .append(sku_occur)
                                   .drop_duplicates(subset='ware_id', keep='first')
                                   .reset_index(drop=True))
                self.ware_index = (self.ware_index
                                   .drop(['cat_1st', 'cat_2nd'], axis=1)
                                   .merge(self.ware_info[['ware_id', 'cat_1st', 'cat_2nd']],
                                          how='left', on='ware_id'))
            else:
                self.ware_index = (sku_occur.drop_duplicates(subset=['ware_id'], keep='first')
                                   .reset_index(drop=True))
            self.ware_index['index'] = self.ware_index.index
            self.ware_index.to_sql('ware_index', wareIndexDb, if_exists='replace', index=False)

    def read_ware_info(self, ware_info_path):
        if os.path.exists(ware_info_path):
            self.ware_info = pd.read_csv(ware_info_path)
        else:
            self.simulate_ware_info()

    def simulate_ware_info(self, ware_info_path):

        ware_count = 10000
        self.ware_info = pd.DataFrame(
             dict(
                 ware_id=list(range(ware_count)),
                 cat_1st=np.repeat(['A', 'B', 'C'], [2000, 3000, 5000]),
                 cat_2nd=np.repeat(["".join(x) for x in itertools.product(['A', 'B', 'C'], ['A', 'B'])],
                                   [1000, 1000, 1000, 2000, 2000, 3000])
             )
        )
        self.ware_info.to_csv(ware_info_path, index=False)
