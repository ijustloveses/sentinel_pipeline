#!/usr/bin/env python
# encoding: utf-8

from multiprocessing import Pool, cpu_count


def m(a, b):
    x = a * b
    return {str(x): x}


def p(a):
    x = a * a * a
    return {str(x): x}


class Word(object):
    def __init__(self, key, varr):
        self.key = key
        self.value = varr

    def update(self, denominator):
        self.value = sum(self.value) / denominator


class WS(object):
    def __init__(self):
        self.words = {}

    def add(self, key, varr):
        self.words[key] = Word(key, varr)


def init():
    ws = WS()
    ws.add('a', [1, 2, 3])
    ws.add('b', [0, 0, 1])
    ws.add('c', [7, 8, 9])
    return ws


def stat(denominator):
    def inner(word):
        word.update(denominator)
        return {word.key: word}
    return inner


ws = init()
pool = Pool(cpu_count())
r = pool.map_async(stat(1), ws.words.itervalues(), callback=ws.words.update)
r.wait()

for key, word in ws.words.iteritems():
    print key, word.value


def update_dict(A):
    def inner(list):
        # print A
        for d in list:
            # print d
            A.update(d)
        return A
    return inner


A = {}
# 这里很简单，就是使用 {'56':56} update  A
r = pool.apply_async(m, (7, 8), callback=A.update)
r.wait()
"""
这里要注意，callback 的调用并不是每一个 map 进程得到结果就调用
而是结果集中到一起，得到一个结果数组，然后一并调用
故此，回调函数的参数实际上是 [{'1': 1}, {'2': 8}, {'3': 27}, ....}]
也就是每个 map 结果组合在一起的列表
"""
r = pool.map_async(p, range(10), callback=update_dict(A))
r.wait()
print A
