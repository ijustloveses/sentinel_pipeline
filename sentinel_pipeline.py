# encoding: utf-8

from multiprocessing import Pool, Manager


class SentinelPipelineNode(object):
    def __init__(self, pipeline, proc, num_cores, sentinel):
        self.manager = pipeline.manager
        self.pool = pipeline.pool
        self.proc = proc
        self.num_cores = num_cores
        self.queue = None
        self.prev_node = None    # 知道前一个节点，以便从其 queue 中读取数据；尾节点还可以知道哨兵已经完成的个数
        self.next_node = None    # 知道后一个节点，以便在本 queue 中为其设置哨兵
        self.sentinel = sentinel
        self.processes = []      # 本节点的全部运行进程，用于等待全部进程完毕，设置哨兵

    def set_next_node(self, node):
        self.next_node = node

    def set_prev_node(self, node):
        self.prev_node = node

    def run(self, *args, **kwargs):
        pass

    def set_sentinels(self):
        """
        注意调用时机
        因为这个函数是 blocking 的
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
        故此，这里的做法是只把用到的成员变量传过来
    """
    for rec in proc(*args, **kwargs):
        q.put(rec)


class SentinelPipelineStart(SentinelPipelineNode):
    """
    可以多进程运行
    每个进程从一个生成器中读取数据，并插入 queue
    """
    def __init__(self, pipeline, proc, num_cores, sentinel):
        super(SentinelPipelineStart, self).__init__(pipeline, proc, num_cores, sentinel)
        self.queue = self.manager.Queue()

    def run(self, *args, **kwargs):
        """
        曾经这样写，但是报错 cPickle.PicklingError: Can't pickle <type 'function'>: attribute lookup __builtin__.function failed
        因为多进程之间要使用pickle来序列化并传递一些数据，但是实例方法并不能被pickle
        能被 pickle 的类型列表： https://docs.python.org/2/library/pickle.html#what-can-be-pickled-and-unpickled

        # args 第一个是 queue，需要提取出来
        def start_proc(*args, **kwargs):
            for rec in self.proc(*args[1:], **kwargs):
                args[0].put(rec)
        # 启动 proc 在 queue 中添加数据
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
        send_q.put(proc(rec, *args, **kwargs))


class SentinelPipelineStep(SentinelPipelineNode):
    """
    可以多进程运行
    每个进程从前一个节点的 queue 中读取数据
    通过转换器转换，把得到的结果插入本节点的 queue 中
    """
    def __init__(self, pipeline, proc, num_cores, sentinel):
        super(SentinelPipelineStep, self).__init__(pipeline, proc, num_cores, sentinel)
        self.queue = self.manager.Queue()

    def run(self, *args, **kwargs):
        """
        # args 前两个分别为 recv 和 send queue
        def invoke_proc(*args, **kwargs):
            while True:
                rec = args[0].get()
                if rec == self.sentinel:
                    return
                args[1].put(self.proc(rec, *args[2:], **kwargs))
        """
        for _ in range(self.num_cores):
            """
            self.processes.append(self.pool.apply_async(invoke_proc, (self.proc, self.prev_node.queue, self.queue,) + args, kwargs))
            """
            self.processes.append(self.pool.apply_async(invoke_proc, (self.prev_node.queue, self.queue, self.proc, self.sentinel,) + args, kwargs))


def end_proc(q, proc, sentinel, *args, **kwargs):

    def data_generator(q, sentinel):
        while True:
            rec = q.get()
            if rec == sentinel:
                return
            yield rec

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
        """
        # args 第一个节点为 recv queue
        def end_proc(*args, **kwargs):
            def data_generator(q):
                while True:
                    rec = q.get()
                    if rec == self.sentinel:
                        return
                    yield rec
            return self.proc(data_generator(args[0]), *args[1:], **kwargs)

        self.processes.append(self.pool.apply_async(end_proc, (self.prev_node.queue,) + args, kwargs))
        """
        self.processes.append(self.pool.apply_async(end_proc, (self.prev_node.queue, self.proc, self.sentinel,) + args, kwargs))


class SentinelPipeline(object):
    """
    内部维护一张流程图，目前图很简单，就是一个列表
    虽然看上去是依次执行，但是其实各个节点会交错运行
    """
    def __init__(self, pool_size=None):
        self.manager = Manager()
        self.pool = Pool()    # TODO
        self.graph = []

    def start_with(self, proc, num_cores, sentinel, *args, **kwargs):
        start = SentinelPipelineStart(self, proc, num_cores, sentinel)
        self.graph.append([start, args, kwargs])

    def pipe_with(self, proc, num_cores, sentinel, *args, **kwargs):
        node = SentinelPipelineStep(self, proc, num_cores, sentinel)
        self.graph[-1][0].set_next_node(node)
        node.set_prev_node(self.graph[-1][0])
        self.graph.append([node, args, kwargs])

    def end_with(self, proc, num_cores, sentinel, *args, **kwargs):
        end = SentinelPipelineEnd(self, proc, num_cores, sentinel)
        self.graph[-1][0].set_next_node(end)
        end.set_prev_node(self.graph[-1][0])
        self.graph.append([end, args, kwargs])

    def run(self):
        # 多进程启动任务节点
        for node, args, kwargs in self.graph:
            node.run(*args, **kwargs)
        # 设置哨兵
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
    return i * 2


# 接收生成器为参数，用于 End 节点
def train(data):
    app = []
    for d in data:
        app.append(d)
    return app


def test_1_simple():
    sp = SentinelPipeline()
    sp.start_with(feed, 1, None)
    sp.pipe_with(process, 8, None)
    sp.end_with(train, 1, None)
    result = sp.run()
    print result


if __name__ == "__main__":
    test_1_simple()
