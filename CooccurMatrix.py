import numpy as np
import os
import sys
import tqdm
import itertools
import pandas as pd
from scipy.sparse import csr_matrix, hstack, vstack


class CooccurMatrix(object):
    def __init__(self, matrix_path):
        self.matrix_path = matrix_path
        self.temp_csr = csr_matrix(np.zeros(1, dtype=np.int64), dtype=np.int64, shape=(1, 1))
        if os.path.isfile(matrix_path):
            self.matrix_cooccur = self.load_sparse_csr(matrix_path)
        else:
            self.matrix_cooccur = csr_matrix((1, 1), dtype=np.int64)

        self.order_ware_sparse_matrix = []

    def save_sparse_csr(self, matrix_path):
        self.matrix_path = matrix_path
        # note that .npz extension is added automatically
        np.savez(self.matrix_path, data=self.matrix_cooccur.data, indices=self.matrix_cooccur.indices,
                 indptr=self.matrix_cooccur.indptr, shape=self.matrix_cooccur.shape)

    def load_sparse_csr(self, matrix_path):
        self.matrix_path = matrix_path
        # here we need to add .npz extension manually
        loader = np.load(self.matrix_path)
        self.matrix_cooccur = csr_matrix((loader['data'], loader['indices'], loader['indptr']),
                                         shape=loader['shape'])
        return self.matrix_cooccur

    def cal_ooccur_matrix(self, new_order_wares, ware_index):
        new_order_wares = new_order_wares[['order_parent_id', 'ware_id', 'index']].drop_duplicates()
        v = ware_index.shape[0]
        self.temp_csr = csr_matrix(np.zeros((v, v), dtype=np.int64), dtype=np.int64, shape=(v, v))
        # 之前用的 int8 导致超过范围变成负数。。。 现在需要改一下
        # need to map each ware(ware_id) to right position in the sparse matrix
        if v > self.matrix_cooccur.shape[0]:
            size_to_add = v - self.matrix_cooccur.shape[0]
            new_rows = csr_matrix((size_to_add, self.matrix_cooccur.shape[0]))
            new_cols = csr_matrix((v, size_to_add))
            self.matrix_cooccur = vstack((self.matrix_cooccur, new_rows))
            self.matrix_cooccur = hstack((self.matrix_cooccur, new_cols))
        elif v < self.matrix_cooccur.shape[0]:
            print("ware index smaller than matrix shape !!!")
            sys.exit("ware index smaller than matrix shape !!!")

        for order_id, grp in tqdm.tqdm(new_order_wares.groupby(['order_id'])):
            comb = tuple(itertools.product(grp['index'], repeat=2))

            row, col = list(zip(*comb))
            data = np.ones(len(comb), dtype=np.int8)
            csr = csr_matrix((data, (row, col)), dtype=np.int8, shape=(v, v))
            self.temp_csr += csr

        self.matrix_cooccur += self.temp_csr

    def cal_order_vs_ware_sparse_matrix(self, new_order_wares, ware_index):
        new_order_wares = new_order_wares[['order_id', 'ware_id', 'index']].drop_duplicates()
        new_order_wares['count'] = 1
        data = new_order_wares['count'].tolist()
        order_parent_id_u = list(sorted(new_order_wares.order_parent_id.unique()))
        order_parent_id_count = len(order_parent_id_u)
        row = new_order_wares.order_parent_id.astype(
            pd.api.types.CategoricalDtype(categories=order_parent_id_u)).cat.codes
        col = new_order_wares['index']

        # should be ware_index_shape[0]
        # ware_count = max(new_order_wares['index']) + 1
        ware_count = ware_index.shape[0]
        self.order_ware_sparse_matrix = csr_matrix((data, (row, col)), shape=(order_parent_id_count, ware_count))

    def cal_cooccur_matrix_new(self, new_order_wares, ware_index):
        self.order_vs_ware_sparse_matrix(new_order_wares, ware_index)
        v = ware_index.shape[0]
        if v > self.matrix_cooccur.shape[0]:
            size_to_add = v - self.matrix_cooccur.shape[0]
            new_rows = csr_matrix((size_to_add, self.matrix_cooccur.shape[0]))
            new_cols = csr_matrix((v, size_to_add))
            self.matrix_cooccur = vstack((self.matrix_cooccur, new_rows))
            self.matrix_cooccur = hstack((self.matrix_cooccur, new_cols))
        elif v < self.matrix_cooccur.shape[0]:
            print("ware index smaller than matrix shape !!!")
            sys.exit("ware index smaller than matrix shape !!!")

        self.matrix_cooccur += self.order_ware_sparse_matrix.T.dot(self.order_ware_sparse_matrix)
