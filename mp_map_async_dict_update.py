#!/usr/bin/env python
# encoding: utf-8

from multiprocessing import Pool, cpu_count


def stat(denominator):
    def inner(word):
        word.update(denominator)
        return {word.key: word}
    return inner


def dstat(word):
    word.update(1)
    return {word.key: word}


def mystat(item):
    denorm, word = item
    word.update(denorm)
    return {word.key: word}


def feeder(denorm, words):
    for word in words:
        yield (denorm, word)


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


def update_dict(A):
    def inner(list):
        print list[0].values()[0].value
        for d in list:
            # print d
            A.update(d)
        return A
    return inner


ws = init()
pool = Pool(cpu_count())

"""
这个不行，因为 stat(1) 不被认为是定义函数，不能 pickle
这里采用回调的方式，结果是不报错，但是回调也不执行，故此很难以调试以及分析内部发生了什么
如果改为 AsyncResult.get 的方式，就可以看到 python 的报错信息了
"""
print "test 1 ..."
r = pool.map_async(stat(1), ws.words.itervalues(), callback=update_dict(ws.words))
r.wait()

for key, word in ws.words.iteritems():
    print key, word.value

"""
这里使用了顶级定义的 dstat，故此可以 pickle 和正常的多进程处理
采用了 callback 的方式，注意 update_dict 的参数 A，并不是每一步 map 的结果，而是全部 map 结果的集合列表
故此，实现了一个 update_dict 函数来处理
注意，update_dict & dstat 需要放在最前面，为该文件模块的顶级函数
"""
print "test 2 ..."
r = pool.map_async(dstat, ws.words.itervalues(), callback=update_dict(ws.words))
r.wait()

for key, word in ws.words.iteritems():
    print key, word.value

"""
类似上面，只不过采用了 AsyncResult.get 而不是回调，故此看到打印结果的流程也和前面不同
"""
# 重新初始化 ws
ws = init()
print "test 3 ..."
r = pool.map_async(dstat, ws.words.itervalues())
A = r.get()

for d in A:
    for k, w in d.iteritems():
        print k, w.value

"""
最后，如果我们一定要传入一个 denominator 而不是写死在 dstat 里面，该如何做呢？
map_async 和 apply_async 不同，不能传入待多进程执行的函数的参数，或者说要求函数参数必须只能是被 map 的元素
那么，当然可以用 apply_async 来改写这个逻辑，但是就丧失掉使用 map_async 比较简单的初衷了
还是限制使用 map_async 的话，可以考虑把 denominator 也放到 map 的元素中
同样，mystat 和 feeder 也要放在最上面

最后，注意，这里使用 callback 的方式就不行，这个就不懂是为啥了 ....
"""


ws = init()
print "test 4 ..."
# r = pool.map_async(mystat, feeder(1, ws.words.itervalues()), callback=update_dict(ws.words))    # Not Working
r = pool.map_async(mystat, feeder(1, ws.words.itervalues()))

for d in r.get():
    for k, w in d.iteritems():
        print k, w.value
# for key, word in ws.words.iteritems():
#    print key, word.value
