# encoding: utf-8

"""
1 个进程写入 qf
若干个进程从 qf 读取数据，double 后写入 qt
1 个进程从 qt 中读取数据

我们在 feed 中，加入了 1 秒延迟
如果我们在多核机器上运行，我们会看到，每秒 feed 一个元素到 qf，都立刻会有 processor 去消费掉这个元素
这回极大提升效率，因为 pipeline 上不同的节点，能够在同一时间交错进行

并不是 并行 feed -> 同步 feed -> 并行 process -> 同步 process -> 并行 train -> 同步 train 这样
不需要同步，全流程节点都是通过 queue 来异步运行
"""

import time
from multiprocessing import Pool
import multiprocessing


def feed(q, num_proc):
    for i in range(10):
        time.sleep(1)
        q.put(i)
    print "feed done"
    for _ in range(num_proc):
        q.put(None)


def process(qf, qt):
    while True:
        i = qf.get()
        print "process {}".format(i)
        if i is None:
            qt.put(None)
            print "one processor done"
            return
        qt.put(i * 2)


def train(q, num_proc):
    app = []
    num_done = 0
    while True:
        i = q.get()
        if i is None:
            num_done += 1
            if num_done == num_proc:
                print "train done"
                return app
        else:
            app.append(i)


if __name__ == '__main__':
    f_manager = multiprocessing.Manager()
    f_q = f_manager.Queue()
    p = Pool()
    t_manager = multiprocessing.Manager()
    t_q = t_manager.Queue()
    p.apply_async(feed, args=(f_q, 8))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    pt = p.apply_async(train, args=(t_q, 8))
    p.close()
    p.join()
    print "all done"
    print pt.get()
