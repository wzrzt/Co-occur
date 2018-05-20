import contextlib
import datetime
import sqlite3

import pandas as pd
import numpy as np


class OrderManager(object):

    def __init__(self, order_source, order_db_path=None,
                 time_begin='2018-01-01 00:00:00', time_end='2018-01-02 23:59:59'):
        self.order_db_path = order_db_path
        self.order_source = order_source
        self.order_ware = pd.DataFrame()
        self.new_orders = pd.DataFrame()
        self.old_order_set = set()
        self.time_interval = dict(time_begin=time_begin, time_end=time_end)

    def read_new_orders(self):
        with contextlib.closing(sqlite3.connect(self.order_db_path)) as orderWareDB:
            self.order_ware = pd.read_sql('select * from new_order_ware', orderWareDB)

    def read_old_orders(self):
        with contextlib.closing(sqlite3.connect(self.order_db_path)) as wareIndexDb:
            old_orders = pd.read_sql("""
                                     SELECT 
                                         * 
                                     FROM old_orders 
                                     WHERE order_time >= %s 
                                         AND order_time <= %s""" % (self.time_interval['time_begin'],
                                                                    self.time_interval['time_end']),
                                     wareIndexDb)
            self.old_order_set = set(old_orders['order_id'])

    def order_ware_fix(self):
        self.order_ware = (self.order_ware[['order_id', 'ware_id', 'order_time']][
                               ~self.order_ware.order_id.isin(self.old_order_set)]
                           .drop_duplicates())

    def simulate_order_ware(self, days_count, order_count, ware_count):
        # generate some orders with wares
        # order_id rule: date + 3 digits numbers
        # begin_date = days_count days ago
        # random seed only work once

        # begin_date = datetime.datetime.now().date() - datetime.timedelta(days=days_count)
        date_list = [datetime.datetime.now().date() -
                     datetime.timedelta(days=days_count - x) for x in range(days_count)]
        begin_date = date_list[0]
        end_date = date_list[-1]
        avg_order_count_day = order_count / days_count
        # 正态分布，再按比例调整到总数是 order_count
        np.random.seed(seed=1)
        order_count_each_day_raw = np.random.normal(avg_order_count_day, avg_order_count_day / 2, days_count)
        order_count_each_day = np.round(order_count_each_day_raw / sum(order_count_each_day_raw) * order_count)
        order_count_each_day[-1] = order_count - sum(order_count_each_day[0:-1])

        order_count_each_day_max = order_count_each_day.max()
        order_count_day_digit_limit = len(str(int(order_count_each_day_max)))
        order_id_each_day = [list(range(1, np.int64(x) + 1)) for x in order_count_each_day]
        np.random.seed(seed=1)
        order_time_sec = [np.random.randint(0, 86399, np.int64(cnt)) for cnt in order_count_each_day]
        orders_df = pd.DataFrame(dict(date_id=np.repeat(date_list, np.int64(order_count_each_day)),
                                      order_id_post_fix=[item for sublist in order_id_each_day for item in sublist],
                                      order_time_sec=[sec for sublist in order_time_sec for sec in sublist]
                                      ))
        orders_df['order_id'] = orders_df.apply(
            lambda x: int('%s%s' % (
                x['date_id'].strftime('%Y%m%d'), str(x['order_id_post_fix']).zfill(order_count_day_digit_limit))),
            axis=1)
        orders_df['order_time'] = orders_df.apply(
            lambda x: '%s %s' % (x['date_id'], datetime.timedelta(seconds=x['order_time_sec'])), axis=1)

        np.random.seed(seed=1)
        ware_count_each_order = np.random.randint(1, 15, order_count)

        np.random.seed(seed=1)
        order_ware = pd.DataFrame(dict(order_id=np.repeat(orders_df['order_id'], ware_count_each_order),
                                       ware_id=np.random.randint(0, ware_count, ware_count_each_order.sum())))
        np.random.seed(seed=1)
        order_ware['ware_num'] = np.random.randint(1, 10, ware_count_each_order.sum())
        self.order_ware = orders_df[['order_id', 'order_time']].merge(order_ware, on='order_id')
        print("Simulated Orders Generated. Data date between %s and %s." % (begin_date, end_date))

    def new_order_to_history(self):

        if self.new_orders.empty:
            print('No new orders.')
            return
        else:
            with contextlib.closing(sqlite3.connect(self.order_db_path)) as wareIndexDb:
                self.new_orders.to_sql('old_orders', wareIndexDb, if_exists='append', index=False)

    def order_ware_add_ware_index(self, ware_index):
        self.order_ware.merge(ware_index[['ware_id', 'ware_index']], how='inner', on='ware_id', inplace=True)

    def set_new_time_interval(self, time_begin, time_end):
        self.time_interval = dict(time_begin=time_begin, time_end=time_end)

    def new_orders_check(self):
        # 读取旧的订单表, 时间在新数据的订单表的最大最小时间范围内
        self.new_orders = (
            self.order_ware[['order_id', 'order_time']][
                ~self.order_ware.order_id.isin(self.old_order_set)]
                .drop_duplicates())

        print("{} New Orders Needs calculating".format(self.new_orders.shape[0]))


if __name__ == "__main__":
    order_manager_1 = OrderManager('simulate')
    order_manager_1.simulate_order_ware(3, 100, 10)
