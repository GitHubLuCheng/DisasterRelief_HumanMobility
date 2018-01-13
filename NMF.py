import math
import numpy as np


# Euclidean distance
def cost_function(i_matrix, j_matrix):
    row_size = len(i_matrix)
    column_size = len(i_matrix[0])
    return sum(pow((i_matrix[i][j] - j_matrix[i][j]), 2) for i in range(row_size) for j in range(column_size))


# Kullback-Leibler divergence
def divergence_function(i_matrix, j_matrix):
    row_size = i_matrix.shape[0]
    column_size = i_matrix.shape[1]
    divergence = 0
    for i in range(row_size):
        for j in range(column_size):
            if i_matrix[i, j] == 0.0:
                divergence += j_matrix[i, j]
            elif j_matrix[i, j] == 0.0:
                divergence += -i_matrix[i, j]
            else:
                divergence += (
                    i_matrix[i, j] * math.log(i_matrix[i, j] / j_matrix[i, j]) - i_matrix[i, j] + j_matrix[i, j])
    return divergence


def update_H(w_matrix, h_matrix, v_matrix):
    row_size = h_matrix.shape[0]
    column_size = h_matrix.shape[1]
    new_h = np.zeros((row_size, column_size), dtype=np.float)
    temp_i = np.dot(np.transpose(w_matrix), v_matrix)
    temp_j = np.dot(np.dot(np.transpose(w_matrix), w_matrix), h_matrix)
    for i in range(row_size):
        for j in range(column_size):
            if temp_j[i, j] == 0.0:
                new_h[i, j] = 0.0
            else:
                new_h[i, j] = h_matrix[i, j] * (temp_i[i, j] / temp_j[i, j])
    return new_h


def update_W(w_matrix, h_matrix, v_matrix):
    row_size = w_matrix.shape[0]
    column_size = w_matrix.shape[1]
    new_w = np.zeros((row_size, column_size), dtype=np.float)
    temp_i = np.dot(v_matrix, np.transpose(h_matrix))
    temp_j = np.dot(np.dot(w_matrix, h_matrix), np.transpose(h_matrix))
    for i in range(row_size):
        for j in range(column_size):
            if temp_j[i, j] == 0.0:
                new_w[i, j] = 0.0
            else:
                new_w[i, j] = w_matrix[i, j] * (temp_i[i, j] / temp_j[i, j])
    return new_w

def matrix_divide(a_matrix, b_matrix):
    result = np.zeros_like(a_matrix, dtype=float)
    for i in range(a_matrix.shape[0]):
        for j in range(a_matrix.shape[1]):
            if b_matrix[i,j] != 0:
                result[i,j] = a_matrix[i, j] / b_matrix[i, j]

    return result
def update_x(a_matrix, x_matrix, p_matrix):
    q_matrix = np.dot(a_matrix, x_matrix)
    # div_matrix = np.divide(p_matrix, q_matrix)
    div_matrix = matrix_divide(p_matrix, q_matrix)
    # with np.errstate(divide='ignore', invalid='ignore'):
    #     div_matrix = np.divide(p_matrix, q_matrix)
    #     div_matrix[div_matrix==np.inf] = 0
    #     div_matrix = np.nan_to_num(div_matrix)
    I = a_matrix.shape[0]
    row_size = x_matrix.shape[0]
    column_size = x_matrix.shape[1]
    memo = {}
    for j in range(row_size):
        for t in range(column_size):
            factor1 = sum((a_matrix[i, j] * div_matrix[i, t]) for i in range(I))
            try:
                factor2 = memo[j]
            except:
                factor2 = sum(a_matrix[i, j] for i in range(I))
                memo[j] = factor2
            x_matrix[j, t] = x_matrix[j, t] * ((factor1 / factor2) if factor2 != 0 else 0)
    return x_matrix


def update_a(a_matrix, x_matrix, p_matrix):
    q_matrix = np.dot(a_matrix, x_matrix)
    div_matrix = matrix_divide(p_matrix, q_matrix)
    # div_matrix = np.divide(p_matrix, q_matrix)
    # with np.errstate(divide='ignore', invalid='ignore'):
    #     div_matrix = np.divide(p_matrix, q_matrix)
    #     div_matrix[div_matrix==np.inf] = 0
    #     div_matrix = np.nan_to_num(div_matrix)
    T = x_matrix.shape[1]
    row_size = a_matrix.shape[0]
    column_size = a_matrix.shape[1]
    memo = {}
    for i in range(row_size):
        for j in range(column_size):
            factor1 = sum((x_matrix[j, t] * div_matrix[i, t]) for t in range(T))
            try:
                factor2 = memo[j]
            except:
                factor2 = sum(x_matrix[j, t] for t in range(T))
                memo[j] = factor2
            a_matrix[i, j] = a_matrix[i, j] * ((factor1 / factor2) if factor2 != 0 else 0)
    return a_matrix


#
# V = np.array([[0.4, 0.3, 0.1], [0.1, 9, 0.5], [7, 0.4, 0.872]], dtype=np.float)
# C = np.array([[0.0, 0.0, 0.8, 0.0], [0.0, 0.0, 0.4, 0.0], [0.0, 0.5, 0.4, 0.0]], dtype=np.float)
# VT = np.transpose(V)
#
# test = np.array([10, -9,2,4,2,1,0,1,2,4,2,4], dtype=np.float)
# print test.max()
# print np.argsort(test)
# # a = np.divide(V, VT)
# b = np.zeros((V.shape[0],V.shape[1]),dtype=np.float)
# for i in range(V.shape[0]):
#     for j in range(V.shape[1]):
#         b[i, j] = V[i, j]/VT[i, j]
# print "The maxtrix a is \n"+ str(a)
# print "The matrix b is \n"+ str(b)
# print "The shape of divide is \n"+str(np.divide(V,np.dot(W,H)))

# k = 2
#
# W = np.random.rand(len(V), k)
# H = np.random.rand(k, len(V[0]))
# print np.sum([[0, 1, 2], [0, 5, 5]], axis=1)
# print W.shape[0]
# for i in range(10):
#     H = update_H(W, H, V)
#     W = update_W(W, H, V)
#     print H
#     print W
#     print np.dot(W, H)
#     print "divergence: " + str(divergence_function(V, np.dot(W, H)))
#     print "distance: " + str(cost_function(V, np.dot(W, H)))
#     print "--------"
#
# """
# # Kullback-Leibler divergence
# V = ax
# """
# k = 2
# a = np.random.rand(len(V), k)
# x = np.random.rand(k, len(V[0]))
# y = np.random.rand(k, len(C[0]))
# for i in range(30):
#     a = update_a(a, x, V)
#     x = update_x(a, x, V)
#     y = update_x(a, y, C)
#     print a
#     print x
#     print y
#     print "---"
#     print np.dot(a, x)
#     print np.dot(a, y)
#     print "divergence 1: " + str(divergence_function(V, np.dot(a, x)))
#     print "divergence 2: " + str(divergence_function(C, np.dot(a, y)))
