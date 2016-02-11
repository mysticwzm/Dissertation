from dispel4py.core import GenericPE
from dispel4py.workflow_graph import WorkflowGraph
from collections import defaultdict
import os, time, math

'''
Variables Definition
'''

nprocs = 2
number_nodes = 5000
niteration = 3
mixing_c = 0.85
random_coeff = (1.0 - mixing_c) / float(number_nodes)
initial_rank = 1.0 / float(number_nodes)
converge_threshold = 1.0 / float(number_nodes) / 10.0
PageRankFile = 'PageRankVector_' + str(number_nodes)
EdgeFilePath = '/home/storm/Zheming/input_' + str(number_nodes)
TempFilePath = '/home/storm/Zheming/temp'
TimeFile = 'time_' + str(number_nodes)


'''
Initial Reader
'''


class InitialRead(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        self._add_output('time')
        self.it = 1
        self.converged = False

    def _process(self, data):
        self.it = data['input']
        if self.it != 1:
            os.chdir(TempFilePath)
            f = open(PageRankFile + '_' + str(self.it - 1), 'r')
            i, rank = f.readline().strip().split('\t')
            if rank.startswith('f'):
                self.converged = True
            f.close()

        if not self.converged:
            os.chdir(EdgeFilePath)
            self.write('time', ['start', self.it])
            filelist = os.listdir(os.getcwd())
            for filename in filelist:
                filenames = os.path.splitext(filename)
                if 'crc' not in filenames[1] and 'SUCCESS' not in filenames[0] \
                and 'PageRank' not in filenames[0] and 'time' not in filenames[0]:
                    f = open(filename, 'r')
                    for line in f:
                        src, dst = line.strip().split('\t')
                        self.write('output', [int(src), dst, self.it])
                        #print src, dst
                    f.close()
            if self.it == 1:
                for i in xrange(number_nodes):
                    self.write('output', [i, 'v' + str(initial_rank), self.it])
            else:
                os.chdir(TempFilePath)
                f = open(PageRankFile + '_' + str(self.it - 1), 'r')
                for line in f:
                    i, rank = line.strip().split('\t')
                    self.write('output', [int(i), rank, self.it])
                f.close()

'''
Stage1
'''


class Stage1(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input', grouping=[0])
        self._add_output('output')
        self.dst_node_list = defaultdict(list)
        self.cur_rank = defaultdict(float)
        self.it = 1

    def _process(self, data):
        src, value, self.it = data['input']
        src = int(src)
        if value.startswith('#'):
            pass

        if not src and not value:
            pass

        if value.startswith('v'):
            value = float(value[1:])
            self.cur_rank[src] = value
        else:
            value = int(value)
            self.dst_node_list[src].append(value)

    def postprocess(self):
        for src in self.cur_rank and self.dst_node_list:
            self.write('output', [src, 's' + str(self.cur_rank[src]), self.it])
            outdeg = len(self.dst_node_list[src])
            cur_rank = self.cur_rank[src] / float(outdeg)
            for dst in self.dst_node_list[src]:
                self.write('output', [dst, 'v' + str(cur_rank), self.it])


'''
Stage2
'''


class Stage2(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input', grouping=[0])
        self._add_output('output')
        self.previous_rank = defaultdict(float)
        self.next_rank = defaultdict(float)
        self.it = 1

    def _process(self, data):
        node, value, self.it = data['input']
        if value.startswith('v'):
            self.next_rank[node] += float(value[1:])
        else:
            self.previous_rank[node] = float(value[1:])

    def postprocess(self):
        for node in self.previous_rank and self.next_rank:
            self.next_rank[node] = self.next_rank[node] * mixing_c + random_coeff
        self.write('output', [self.previous_rank, self.next_rank, self.it])


'''
Checker and Reader
'''


class Check(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        #self.numprocesses = nprocs
        self.it = 1
        self._add_input('input', grouping='global')
        # if self.it != niteration:
        #     self._add_output('output')
        self._add_output('time')
        self.previous_rank = [0.0] * number_nodes
        self.next_rank = [0.0] * number_nodes

    def _process(self, data):
        previous_rank, next_rank, self.it = data['input']
        for node in previous_rank and next_rank:
            self.previous_rank[node] = previous_rank[node]
            self.next_rank[node] = next_rank[node]

    def postprocess(self):
        change_reported = 0
        for i in xrange(number_nodes):
            diff = math.fabs(self.previous_rank[i] - self.next_rank[i])
            if diff > converge_threshold:
                change_reported += 1

        print 'Iteration = %d, changed reducer = %d' % (self.it, change_reported)

        if change_reported == 0:
            print 'RageRank vector converged. Now preparing to finish...'
        
        if self.it == niteration:
            print 'Reached the max iteration. Now preparing to finish...'
        
        if change_reported == 0 or self.it == niteration:
            os.chdir(TempFilePath)
            f = open(PageRankFile + '_' + str(self.it), 'w')
            f.truncate()
            for i in xrange(number_nodes):
                f.write('%d\tf%f\n' % (i, self.next_rank[i]))
            f.close()
            self.write('time', ['end', self.it])
        
        if change_reported > 0 and self.it != niteration:
            os.chdir(TempFilePath)
            # filelist = os.listdir(os.getcwd())
            # for filename in filelist:
            #     filenames = os.path.splitext(filename)
            #     if 'crc' not in filenames[1] and 'SUCCESS' not in filenames[0]:
            #         f = open(filename, 'r')
            #         for line in f:
            #             src, dst = line.strip().split('\t')
            #             self.write('output', [int(src), dst])
            #         f.close()
            f = open(PageRankFile + '_' + str(self.it), 'w')
            f.truncate()
            for i in xrange(number_nodes):
                #print i, self.next_rank[i]
                f.write('%d\tv%f\n' % (i, self.next_rank[i]))
            f.close()
            self.write('time', ['end', self.it])

'''
Timer
'''


class Timer(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('time', grouping='global')
        self.start_time = 0.0
        self.end_time = 0.0
        self.it = 1

    def _process(self, data):
        operation, self.it = data['time']
        if operation == 'start':
            self.start_time = time.time()
        elif operation == 'end':
            self.end_time = time.time()

    def postprocess(self):
        previous_time = 0.0
        os.chdir(TempFilePath)
        if self.it != 1:
            f = open(TimeFile + '_' + str(self.it - 1), 'r')
            previous_time = float(f.readline())
            f.close()
        total_time = previous_time + (self.end_time - self.start_time)
        f = open(TimeFile + '_' + str(self.it), 'w')
        f.truncate()
        f.write(str(total_time))
        f.close()
        print 'Total time : ', total_time

'''
Set up Workflow Graph
'''

InitialReader = InitialRead()
InitialReader.numprocesses = 1
StageOne = Stage1()
StageOne.numprocesses = nprocs + 1
StageTwo = Stage2()
StageTwo.numprocesses = nprocs
Checker = Check()
Checker.numprocesses = 1
GlobalTimer = Timer()
GlobalTimer.numprocesses = 1

graph = WorkflowGraph()
graph.connect(InitialReader, 'output', StageOne, 'input')
graph.connect(InitialReader, 'time', GlobalTimer, 'time')
graph.connect(StageOne, 'output', StageTwo, 'input')
graph.connect(StageTwo, 'output', Checker, 'input')
graph.connect(Checker, 'time', GlobalTimer, 'time')

#dispel4py multi PageRank.py -n 6 -d '{"InitialRead" : [{"input" : 1}]}'
