# encoding: utf-8

"""
若干个进程写入 qf
若干个进程从 qf 读取数据，double 后写入 qt
1 个进程从 qt 中读取数据

我们希望在全部 feed 进程都处理完之后，再统一给 process 进程发送哨兵 sentinel
而且希望这个逻辑加入之后，不会影响全部流程节点的异步性。就是说全部流程节点仍然可以交错进行，而不是每一步完成都要同步后，才能进行下一步

我们在 feed 中，加入了 1 秒延迟
如果我们在多核机器上运行，我们会看到，每秒 feed 一个元素到 qf，都立刻会有 processor 去消费掉这个元素
这回极大提升效率，因为 pipeline 上不同的节点，能够在同一时间交错进行

这里一定要注意加 sentinel 的位置，见最后部分代码的注释
"""

import time
from multiprocessing import Pool
import multiprocessing


def feed(q):
    for i in range(10):
        time.sleep(1)
        q.put(i)
    print "feed done"


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
    # 启动两个 feed 进程
    f1 = p.apply_async(feed, args=(f_q, ))
    f2 = p.apply_async(feed, args=(f_q, ))
    """
    注意，f1.get() & f2.get() 的位置不能放在这里，否则同步调用就会 block 住 process 的执行
    变成先走完全部 feed 流程，数字全部入 queue 之后，才开始 process；而不是 feed 和 process 交错执行了
    """
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    p.apply_async(process, args=(f_q, t_q))
    pt = p.apply_async(train, args=(t_q, 8))
    f1.get()
    f2.get()
    for _ in range(8):
        f_q.put(None)
    """
    由于要想 queue 中插入 sentinel，故此不能在 p close 之后执行
    """
    p.close()
    p.join()
    print "all done"
    print pt.get()
