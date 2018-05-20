
import numpy as np
from CooccurMatrix import CooccurMatrix
from OrderManager import OrderManager
from WareIndex import WareIndex
from scipy.sparse import csr_matrix


class WareEmbedding(object):

    def __init__(self, order_source='simulate', order_db_path=None, ware_index_db_path=None, matrix_path=None,
                 ware_info_path=None):
        self.CooccurMatix = CooccurMatrix()
        self.OrderManager = OrderManager(order_source, order_db_path)
        self.WareIndex = WareIndex(ware_index_db_path, ware_info_path)
        self.order_db_path = order_db_path
        self.ware_index_db_path = ware_index_db_path
        self.matrix_path = matrix_path
        self.order_source = order_source
        self.matrix = csr_matrix(np.zeros(1, dtype=np.int64), dtype=np.int64, shape=(1, 1))

    def calculate_cooccur_matrix(self):

        if self.order_source == 'simulate':
            self.OrderManager.simulate_order_ware()
        elif self.order_source == 'sqlite':
            self.OrderManager.read_new_orders()
        else:
            print("Error data source, can't get orders")
        if self.OrderManager.order_ware.is_empty:
            print('No Order data input')
        else:
            self.WareIndex.add_wares()
        # This new methods is 200 times faster than the old one.
            self.CooccurMatix.cal_cooccur_matrix_new()

    def save_matrix(self):
        self.CooccurMatix.save_sparse_csr(self.matrix_path)


if __name__ == '__main__':
    ware_embedding = WareEmbedding(order_source='simulate')
    ware_embedding.calculate_coooccur_matrix()
    ware_embedding.save_matrix()
