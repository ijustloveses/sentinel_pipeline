# encoding: utf-8

"""
本项目基于 multiprocessing 中的 Pool / Queue / Manager，并使用了哨兵机制，来实现一个多进程任务流转管道
目前只支持单线型的管道，最后一个节点必须为单进程；每个节点都通过哨兵来判断前面的节点是否已经运行完毕

由于采用的是 Queue 和 Pool.apply_async，故此，调用的函数、函数的参数都通过 pickle 来序列化，故此最好定义在最外层，也不能使用 decorator
比如 Pool.apply_async(proc, [args, [kwds, [callback]]])，这里面的 proc, callback 以及在 args 或者 kwds 中的变量和函数统统都要被 pickle
而 python 目前能够被序列化的类型有限，尤其是对于函数来说，类成员函数不能被 pickle，不是在文件最外层定义的函数也不能被 pickle
而且如果是一个类实例对象，而这个对象中含有 Manager / Pool / AsyncResult 或其他复杂的对象为成员变量的话，很可能也不能被 pickle
故此，这里采取了一些曲折的方法，最终实现了这个多进程运行框架

参见：
https://stackoverflow.com/questions/25071910/multiprocessing-pool-calling-helper-functions-when-using-apply-asyncs-callback
https://stackoverflow.com/questions/8804830/python-multiprocessing-pickling-error
"""

from multiprocessing import Pool, Manager, cpu_count


class SentinelPipelineNode(object):
    def __init__(self, pipeline, proc, num_cores, sentinel):
        self.manager = pipeline.manager
        self.pool = pipeline.pool
        self.proc = proc
        self.num_cores = num_cores
        self.queue = None
        self.prev_node = None     # 知道前一个节点，以便从其 queue 中读取数据；尾节点还可以知道哨兵已经完成的个数
        self.next_node = None     # 知道后一个节点，以便在本 queue 中为其设置哨兵
        self.sentinel = sentinel  # 哨兵变量，前一个节点需要设置本节点的哨兵变量
        self.processes = []       # 本节点的全部运行进程，用于等待全部进程完毕，设置哨兵

    def set_next_node(self, node):
        self.next_node = node

    def set_prev_node(self, node):
        self.prev_node = node

    def run(self, *args, **kwargs):
        pass

    def set_sentinels(self):
        """
        注意调用时机，这个函数是 blocking ( p.get() ) 的
        不是所有步骤的节点同时设置哨兵，而是每个节点运行完毕 (p.get() block 运行完毕) 后，才设置下一步骤的哨兵
        不过，反正第二步不可能比第一步更早结束，故此没有问题
        """
        # End 节点为单进程就不需要安置哨兵了
        if self.next_node is None:
            return
        # 等待全部结束
        for p in self.processes:
            p.get()
        # 创建哨兵
        for _ in range(self.next_node.num_cores):
            self.queue.put(self.next_node.sentinel)


def start_proc(q, proc, *args, **kwargs):
    """ 注意，这里也不能把 node 自身直接通过 apply_async 传过来，因为其内部含有很多不能 pickle 的成员变量
        故此，这里的做法是只把用到的成员变量传过来；注意，这个 proc 必须是在文件的最外层定义
    """
    for rec in proc(*args, **kwargs):
        q.put(rec)


class SentinelPipelineStart(SentinelPipelineNode):
    """
    可以多进程运行
    每个进程从一个生成器中读取数据，并插入 queue
    不过，由于多个进程共享 *args 和 **kwarg，故此，通常只是单进程使用
    """
    def __init__(self, pipeline, proc, num_cores, sentinel):
        super(SentinelPipelineStart, self).__init__(pipeline, proc, num_cores, sentinel)
        self.queue = self.manager.Queue()

    def run(self, *args, **kwargs):
        """
        曾经像下面这样写，但是报错 cPickle.PicklingError: Can't pickle <type 'function'>: attribute lookup __builtin__.function failed
        因为多进程之间要使用pickle来序列化并传递一些数据，但是实例方法并不能被pickle
        能被 pickle 的类型列表： https://docs.python.org/2/library/pickle.html#what-can-be-pickled-and-unpickled

        def start_proc(*args, **kwargs):    # 这是一个内层定义的函数，无法被 pickle
            for rec in self.proc(*args[1:], **kwargs):
                args[0].put(rec)
        for _ in range(self.num_cores):
            # apply_async 要求直接传 args 和 kwargs，而不是 *args & **kwargs
            # 把 queue 也传到进程里，multiprocess.Queue 有 pickle 方法可以被序列化
            # 否则，在进程中直接读取 self.queue 这样是不行的
            self.processes.append(self.pool.apply_async(start_proc, (self.queue,) + args, kwargs))
        """
        for _ in range(self.num_cores):
            self.processes.append(self.pool.apply_async(start_proc, args=(self.queue, self.proc,) + args, kwds=kwargs))


def invoke_proc(recv_q, send_q, proc, sentinel, *args, **kwargs):
    while True:
        rec = recv_q.get()
        if rec == sentinel:
            return
        for processed in proc(rec, *args, **kwargs):
            send_q.put(processed)


class SentinelPipelineStep(SentinelPipelineNode):
    """
    中间节点之一，可以多进程运行
    每个进程从前一个节点的 queue 中读取数据
    通过转换器转换，把得到的结果插入本节点的 queue 中
    注意，该类节点每次从 queue 中取出一个数据，就会处理得到一个结果，并把结果放到本节点 queue 中
    """
    def __init__(self, pipeline, proc, num_cores, sentinel):
        super(SentinelPipelineStep, self).__init__(pipeline, proc, num_cores, sentinel)
        self.queue = self.manager.Queue()

    def run(self, *args, **kwargs):
        for _ in range(self.num_cores):
            self.processes.append(self.pool.apply_async(invoke_proc, (self.prev_node.queue, self.queue, self.proc, self.sentinel,) + args, kwargs))


def data_generator(q, sentinel):
    """
    包装从 queue 中取数据的流程，形成一个 generator
    """
    while True:
        rec = q.get()
        if rec == sentinel:
            return
        yield rec


def aggreg_proc(recv_q, send_q, proc, sentinel, *args, **kwargs):
    """
    从生成器中读取数据，进行处理，最终把全部数据得到一个结果
    最后把这个结果入 queue
    """
    send_q.put(proc(data_generator(recv_q, sentinel), *args, **kwargs))


class SentinelPipelineAggreg(SentinelPipelineNode):
    """
    中间节点之一，可以多进程运行
    每个进程从前一个节点的 queue 中读取数据
    通过累积器转换，把对全部数据的处理结果插入本节点的 queue 中
    注意，该类节点每次从 queue 中取出一个数据，然后进行累积计算，不停累积，期间并不入 queue；最后把最终累积结果放到本节点 queue 中
    """
    def __init__(self, pipeline, proc, num_cores, sentinel):
        super(SentinelPipelineAggreg, self).__init__(pipeline, proc, num_cores, sentinel)
        self.queue = self.manager.Queue()

    def run(self, *args, **kwargs):
        for _ in range(self.num_cores):
            self.processes.append(self.pool.apply_async(aggreg_proc, (self.prev_node.queue, self.queue, self.proc, self.sentinel,) + args, kwargs))


def end_proc(q, proc, sentinel, *args, **kwargs):
    return proc(data_generator(q, sentinel), *args, **kwargs)


class SentinelPipelineEnd(SentinelPipelineNode):
    """
    目前要求 End 节点一定是单进程的，故此不需要 queue
    End 节点中的 proc 会接收一个生成器，并读取其中数据，进而返回处理结果
    这个生成器就是从前一个节点中 queue 读取的
    """
    def __init__(self, pipeline, proc, num_cores, sentinel):
        super(SentinelPipelineEnd, self).__init__(pipeline, proc, num_cores, sentinel)
        self.num_cores = 1

    def run(self, *args, **kwargs):
        # 单进程
        self.processes.append(self.pool.apply_async(end_proc, (self.prev_node.queue, self.proc, self.sentinel,) + args, kwargs))


class SentinelPipeline(object):
    """
    内部维护一张流程图，目前图很简单，就是一个列表
    虽然看上去是依次执行，但是其实各个节点会交错运行
    """
    def __init__(self, pool_size=None):
        self.manager = Manager()
        self.pool = Pool(cpu_count())
        self.graph = []

    def start_with(self, proc, num_cores, sentinel, *args, **kwargs):
        start = SentinelPipelineStart(self, proc, num_cores, sentinel)
        self.graph.append([start, args, kwargs])

    def pipe_with(self, proc, num_cores, sentinel, nodetype, *args, **kwargs):
        if nodetype == "step":
            node = SentinelPipelineStep(self, proc, num_cores, sentinel)
        elif nodetype == "end":
            node = SentinelPipelineEnd(self, proc, num_cores, sentinel)
        elif nodetype == "aggreg":
            node = SentinelPipelineAggreg(self, proc, num_cores, sentinel)
        else:
            raise RuntimeError("wrong node type")
        self.graph[-1][0].set_next_node(node)
        node.set_prev_node(self.graph[-1][0])
        self.graph.append([node, args, kwargs])

    def run(self):
        # 多进程启动任务节点
        for node, args, kwargs in self.graph:
            node.run(*args, **kwargs)
        # 每个步骤节点运行完毕后，设置哨兵；这里是个阻塞调用
        for node, _, _ in self.graph:
            node.set_sentinels()
        self.pool.close()
        self.pool.join()
        return self.graph[-1][0].processes[0].get()


"""  Test 1. 简单的测试

1 个 feed =>  8 个 process  =>  1 个 train 的图

"""


# 生成器，用于 Start 节点
def feed():
    for i in range(10):
        yield i


# 转换器，用于 Step 节点
def process(i):
    yield i * 2


# 接收生成器为参数，用于 End 节点
def train(data):
    app = []
    for d in data:
        app.append(d)
    return app


def test_1_simple():
    print "----------------- test 1 --------------------"
    sp = SentinelPipeline()
    sp.start_with(feed, 1, None)
    sp.pipe_with(process, 8, None)
    sp.end_with(train, 1, None)
    result = sp.run()
    print result


"""  Test 2. 多个慢速 feeder

2 个 feed =>  8 个 process  =>  1 个 train 的图

feeder 随机睡眠，这里意在测试流程各节点可以交错运行，而不是一个节点上的多个进程都运行完毕才能进行下一个节点

"""


# 生成器，用于 Start 节点
def feed2():
    import random
    import time
    for i in range(10):
        time.sleep(random.randint(1, 5))
        print "feed: {}".format(i)
        yield i


# 转换器，用于 Step 节点
def process2(i):
    print "process: {}".format(i)
    yield i * 2


def test_2_simple():
    print "----------------- test 2 --------------------"
    sp = SentinelPipeline()
    sp.start_with(feed2, 2, None)
    sp.pipe_with(process2, 8, None)
    sp.end_with(train, 1, None)
    result = sp.run()
    print result


if __name__ == "__main__":
    test_1_simple()
    test_2_simple()
