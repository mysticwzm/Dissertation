from dispel4py.base import IterativePE
from dispel4py.core import GenericPE
from dispel4py.workflow_graph import WorkflowGraph
import time, random, os, string

'''
Variables definition
'''

CLASS = 'S'
NUM_PROCS = 1 << 3
nprocs = (NUM_PROCS >> 2) - 1
FilePath = '/exports/work/inf_dir/src/dispel4py/Zheming'
ArrayFile = 'array_' + CLASS + '_' + str(NUM_PROCS)
TimeFile = 'time_' + CLASS + '_' + str(NUM_PROCS)

class_configuration_dic = {'S': {'TOTAL_KEYS_LOG_2': 16, 'MAX_KEY_LOG_2': 11, 'NUM_BUCKETS_LOG_2': 9, 'MIN_PROCS': 1},
                           'W': {'TOTAL_KEYS_LOG_2': 20, 'MAX_KEY_LOG_2': 16, 'NUM_BUCKETS_LOG_2': 10, 'MIN_PROCS': 1},
                           'A': {'TOTAL_KEYS_LOG_2': 23, 'MAX_KEY_LOG_2': 19, 'NUM_BUCKETS_LOG_2': 10, 'MIN_PROCS': 1},
                           'B': {'TOTAL_KEYS_LOG_2': 25, 'MAX_KEY_LOG_2': 21, 'NUM_BUCKETS_LOG_2': 10, 'MIN_PROCS': 1},
                           'C': {'TOTAL_KEYS_LOG_2': 27, 'MAX_KEY_LOG_2': 23, 'NUM_BUCKETS_LOG_2': 10, 'MIN_PROCS': 1},
                           'D': {'TOTAL_KEYS_LOG_2': 29, 'MAX_KEY_LOG_2': 27, 'NUM_BUCKETS_LOG_2': 10, 'MIN_PROCS': 4}}

TOTAL_KEYS_LOG_2 = class_configuration_dic[CLASS]['TOTAL_KEYS_LOG_2']
MAX_KEY_LOG_2 = class_configuration_dic[CLASS]['MAX_KEY_LOG_2']
NUM_BUCKETS_LOG_2 = class_configuration_dic[CLASS]['NUM_BUCKETS_LOG_2']
MIN_PROCS = class_configuration_dic[CLASS]['MIN_PROCS']

TOTAL_KEYS = 1 << TOTAL_KEYS_LOG_2
MAX_KEY = 1 << MAX_KEY_LOG_2
NUM_BUCKETS = 1 << NUM_BUCKETS_LOG_2
NUM_KEYS = TOTAL_KEYS / NUM_PROCS * MIN_PROCS

SIZE_OF_BUFFER = 13 * NUM_KEYS / 2
if NUM_PROCS < 256:
    SIZE_OF_BUFFER = 3 * NUM_KEYS / 2
elif NUM_KEYS < 512:
    SIZE_OF_BUFFER = 5 * NUM_KEYS / 2
elif NUM_KEYS < 1024:
    SIZE_OF_BUFFER = 4 * NUM_KEYS

MAX_PROCS = 1024
if CLASS == 'S':
    MAX_PROCS = 128
MAX_ITERATIONS = 10
TEST_ARRAY_SIZE = 5

#T_TOTAL = 0
#T_RANK = 1
#T_RCOMM = 2
#T_VERIFY = 3
#T_LAST = 3

# key_buff_ptr_global = None
# total_local_keys = None
# total_lesser_keys = None

# passed_verification = 0
# my_iteration = 0

test_index_array_dic = {'S': [48427, 17148, 23627, 62548, 4431],
                        'W': [357773, 934767, 875723, 898999, 404505],
                        'A': [2112377, 662041, 5336171, 3642833, 4250760],
                        'B': [41869, 812306, 5102857, 18232239, 26860214],
                        'C': [44172927, 72999161, 74326391, 129606274, 21736814],
                        'D': [1317351170, 995930646, 1157283250, 1503301535, 1453734525]}
test_rank_array_dic = {'S': [0, 18, 346, 64917, 65463],
                       'W': [1249, 11698, 1039987, 1043896, 1048018],
                       'A': [104, 17523, 123928, 8288932, 8388264],
                       'B': [33422937, 10244, 59149, 33135281, 99],
                       'C': [61147, 882988, 266290, 133997595, 133525895],
                       'D': [1, 36538729, 1978098519, 2145192618, 2147425337]}

test_index_array = test_index_array_dic[CLASS]
test_rank_array = test_rank_array_dic[CLASS]

# process_bucket_distrib_ptr1 = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
# process_bucket_distrib_ptr2 = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
# bucket_size_totals = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
shift = MAX_KEY_LOG_2 - NUM_BUCKETS_LOG_2
# largest_local_key = [0] * MAX_PROCS

'''
Random number generator
'''

'''
def randlc(x, a):
    if not hasattr(randlc, 'KS'):
        randlc.KS = 0
    if not hasattr(randlc, 'R23'):
        randlc.R23 = 0.0
    if not hasattr(randlc, 'R46'):
        randlc.R46 = 0.0
    if not hasattr(randlc, 'T23'):
        randlc.T23 = 0.0
    if not hasattr(randlc, 'T46'):
        randlc.T46 = 0.0

    if randlc.KS == 0:
        randlc.R23 = 1.0
        randlc.R46 = 1.0
        randlc.T23 = 1.0
        randlc.T46 = 1.0
        for i in xrange(23):
            randlc.R23 *= 0.50
            randlc.T23 *= 2.0
        for i in xrange(46):
            randlc.R46 *= 0.50
            randlc.T46 *= 2.0
        randlc.KS = 1

    t1 = randlc.R23 * a[0]
    j = t1
    a1 = float(int(j))
    a2 = a[0] - randlc.T23 * a1

    t1 = randlc.R23 * x[0]
    j = t1
    x1 = float(int(j))
    x2 = x[0] - randlc.T23 * x1
    t1 = a1 * x2 + a2 * x1

    j = randlc.R23 * t1
    t2 = float(int(j))
    z = t1 - randlc.T23 * t2
    t3 = randlc.T23 * z + a2 * x2
    j = randlc.R46 * t3
    t4 = float(int(j))
    x[0] = t3 - randlc.T46 * t4

    return randlc.R46 * x[0]
'''

'''
Seed generator
'''

'''
def find_my_seed(kn, np, nn, s, a):
    nq = nn / np
    mq = 0
    while nq > 1:
        mq += 1
        nq /= 2

    t1 = [a]

    for i in xrange(mq):
        t2 = randlc(t1, t1)

    an = t1[0]
    kk = kn
    t1 = [s]
    t2 = [an]

    for i in xrange(1,101):
        ik = kk / 2
        if 2 * ik != kk:
            t3 = randlc(t1, t2)
        if ik == 0:
            break
        t3 = randlc(t2, t2)
        kk = ik

    return t1[0]
'''

'''
Sequence generator
'''

'''
def create_seq(seed, a):
    k = MAX_KEY / 4
    sequence = []

    seed = [seed]
    a = [a]
    for i in xrange(NUM_KEYS):
        x = randlc(seed, a)
        x += randlc(seed, a)
        x += randlc(seed, a)
        x += randlc(seed, a)

        sequence.append(int(k * x))

    return sequence
'''

'''
Main workflow components
'''

'''
Initializer
'''


class Init(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        #self._add_output('time')
        self.key_arrays = []
        self.it = 1

    def _process(self, data):
        # for my_iteration in xrange(MAX_ITERATIONS):
        self.it = data['input']
        os.chdir(FilePath)
        f = open(ArrayFile)
        for line in f:
            key_array = []
            my_rank, array = line.strip().split('\t')
            my_rank = int(my_rank)
            #array = array.replace('[','').replace(']','')
            for num in array.strip().split(' '):
                key_array.append(int(num))
            self.write('output', [my_rank, key_array, self.it])
            # for my_rank in xrange(NUM_PROCS):
            #     key_array = create_seq(find_my_seed(my_rank,
            #                                         NUM_PROCS,
            #                                         4 * long(TOTAL_KEYS) * MIN_PROCS,
            #                                         314159265.00,
            #                                         1220703125.00),
            #                            1220703125.00)


'''
Bucket setter
'''


class BucketSet(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('bucket_size')
        self._add_output('output')
        self._add_output('time')

    def _process(self, data):
        my_rank, key_array, it = data['input']
        self.write('time', [my_rank, 'start', it])
        if my_rank == 0:
            for i in xrange(1, it + 1):
                key_array[i] = i
                key_array[i + MAX_ITERATIONS] = MAX_KEY - i

        bucket_size = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
        # bucket_size_totals = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
        key_buff1 = [0] * SIZE_OF_BUFFER
        bucket_ptrs = [0] * NUM_BUCKETS

        for i in xrange(TEST_ARRAY_SIZE):
            if test_index_array[i] / NUM_KEYS == my_rank:
                bucket_size[NUM_BUCKETS + i] = key_array[test_index_array[i] % NUM_KEYS]

        for i in xrange(NUM_KEYS):
            bucket_size[key_array[i] >> shift] += 1

        for i in xrange(1, NUM_BUCKETS):
            bucket_ptrs[i] = bucket_ptrs[i - 1] + bucket_size[i - 1]

        for i in xrange(NUM_KEYS):
            key = key_array[i]
            key_buff1[bucket_ptrs[key >> shift]] = key
            bucket_ptrs[key >> shift] += 1

        self.write('bucket_size', bucket_size)
        self.write('output', [my_rank, bucket_size, key_buff1, it])


'''
Bucket size aggregator
'''


class BucketSizeAggregate(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('bucket_size', grouping='global')
        self._add_output('bucket_size_totals')
        #self._add_output('time')
        self.bucket_size_totals = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)

    def _process(self, data):
        bucket_size = data['bucket_size']
        for i in xrange(NUM_BUCKETS + TEST_ARRAY_SIZE):
            self.bucket_size_totals[i] += bucket_size[i]

    def postprocess(self):
        # total_key = 0
        # for i in xrange(NUM_BUCKETS):
        #     total_key += self.bucket_size_totals[i]
        # if total_key:
        #     print total_key
        #     print self.bucket_size_totals
            
        self.write('bucket_size_totals', self.bucket_size_totals)


'''
Bucket re-distributor (sender part)
'''


class BucketRedistributeSend(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('bucket_size_totals', grouping='all')
        self._add_input('input')
        self._add_output('output')
        #self._add_output('time')
        self.bucket_size = []
        self.bucket_size_totals = []
        self.my_rank = []
        self.key_buff1 = []
        self.it = 1
        # self.key_buff2 = [[]] * NUM_PROCS
        # self.process_bucket_distrib_ptr1 = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
        # self.process_bucket_distrib_ptr2 = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
        # self._add_output('rank&send_count&send_displ')

    def _process(self, data):
        # self.bucket_size_totals = []
        if 'bucket_size_totals' in data:
            self.bucket_size_totals = data['bucket_size_totals']
        if 'input' in data:
            my_rank, bucket_size, key_buff1, self.it = data['input']
            self.my_rank.append(my_rank)
            self.bucket_size.append(bucket_size)
            self.key_buff1.append(key_buff1)
        while self.my_rank and self.bucket_size_totals:
            process_bucket_distrib_ptr1 = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
            process_bucket_distrib_ptr2 = [0] * (NUM_BUCKETS + TEST_ARRAY_SIZE)
            my_rank = self.my_rank.pop()
            bucket_size = self.bucket_size.pop()
            key_buff1 = self.key_buff1.pop()
            bucket_sum_accumulator = 0
            local_bucket_sum_accumulator = 0

            send_displ = [0] * MAX_PROCS
            send_count = [0] * MAX_PROCS

            j = 0
            for i in xrange(NUM_BUCKETS):
                bucket_sum_accumulator += self.bucket_size_totals[i]
                local_bucket_sum_accumulator += bucket_size[i]
                if bucket_sum_accumulator >= (j + 1) * NUM_KEYS:
                    send_count[j] = local_bucket_sum_accumulator
                    if j != 0:
                        send_displ[j] = send_displ[j - 1] + send_count[j - 1]
                        process_bucket_distrib_ptr1[j] = process_bucket_distrib_ptr2[j - 1] + 1
                    process_bucket_distrib_ptr2[j] = i
                    j += 1
                    local_bucket_sum_accumulator = 0
            while j < NUM_PROCS:
                send_count[j] = 0
                process_bucket_distrib_ptr1[j] = 1
                j += 1
            last_send_displ_start = send_displ[NUM_PROCS - 1]
            last_send_displ_end = send_displ[NUM_PROCS - 1] + send_count[NUM_PROCS - 1]

            for i in xrange(NUM_PROCS - 1):
                self.write('output',[i, key_buff1[send_displ[i]: send_displ[i + 1]], \
                                     process_bucket_distrib_ptr1, \
                                     process_bucket_distrib_ptr2, \
                                     self.bucket_size_totals, self.it])

            self.write('output', [NUM_PROCS - 1, \
                                  key_buff1[last_send_displ_start: last_send_displ_end], \
                                  process_bucket_distrib_ptr1, \
                                  process_bucket_distrib_ptr2, \
                                  self.bucket_size_totals, self.it])
            # self.write('rank&send_count&send_displ', (self.my_rank, send_count, send_displ))
    #         for i in xrange(NUM_PROCS - 1):
    #             self.key_buff2[i] = self.key_buff2[i] + key_buff1[send_displ[i]: send_displ[i + 1]]
    #         self.key_buff2[NUM_PROCS - 1] = self.key_buff2[NUM_PROCS - 1] + key_buff1[last_send_displ_start: last_send_displ_end]
    #         self.process_bucket_distrib_ptr1 = process_bucket_distrib_ptr1
    #         self.process_bucket_distrib_ptr2 = process_bucket_distrib_ptr2


    # def postprocess(self):
    #     for i in xrange(NUM_PROCS):
    #         self.write('output', [i, self.key_buff2[i], \
    #                               self.process_bucket_distrib_ptr1, self.process_bucket_distrib_ptr2, \
    #                               self.bucket_size_totals, self.it])


'''
Bucket re-distributor (receiver part)
'''


class BucketRedistributeRecv(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input', grouping=[0])
        self._add_output('output')
        #self._add_output('time')
        # self._add_output('min&max_val')
        self.key_buff2 = [[]] * NUM_PROCS
        self.process_bucket_distrib_ptr1 = []
        self.process_bucket_distrib_ptr2 = []
        self.bucket_size_totals = []
        self.it = 1

    def _process(self, data):
        dest, subarray, \
        self.process_bucket_distrib_ptr1, \
        self.process_bucket_distrib_ptr2, \
        self.bucket_size_totals, self.it = data['input']
        self.key_buff2[dest] = self.key_buff2[dest] + subarray

    def postprocess(self):
        for i in xrange(NUM_PROCS):
            if self.key_buff2[i]:
                min_key_val = self.process_bucket_distrib_ptr1[i] << shift
                max_key_val = ((self.process_bucket_distrib_ptr2[i] + 1) << shift) - 1
                self.write('output', [i, self.key_buff2[i], \
                                      self.process_bucket_distrib_ptr1, \
                                      self.process_bucket_distrib_ptr2, \
                                      min_key_val, max_key_val, \
                                      self.bucket_size_totals, self.it])
                # self.write('min&max_val', (min_key_val, max_key_val))


'''
Min and Max value aggregator
'''

'''
class min_max_aggregate(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('min&max_val', grouping='global')
        self._add_output('total_min&max_val')
        self.min_key_val = [0] * MAX_PROCS
        self.max_key_val = [0] * MAX_PROCS

    def _process(self, data):
        min_key_val, max_key_val = data['min&max_val']
        for i in xrange(MAX_PROCS):
            self.min_key_val[i] += min_key_val[i]
            self.max_key_val[i] += max_key_val[i]

    def postprocess(self):
        self.write('total_min&max_val', (self.min_key_val, self.max_key_val))
'''

'''
Partial verification
'''


class PartialVerify(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('time')
        #self._add_output('output')
        #self._add_output('verified')
        self.process_bucket_distrib_ptr1 = []
        self.process_bucket_distrib_ptr2 = []
        self.min_key_val = 0
        self.max_key_val = 0
        self.bucket_size_totals = []
        self.my_rank = 0
        self.key_buff2 = []
        self.vcounter = 0
        self.it = 1

    def _process(self, data):
        # if 'total_min&max_val' in data:
        # self.min_key_val, self.max_key_val = data['total_min&max_val']
        # if 'bucket_size_totals' in data:
        # self.bucket_size_totals = data['bucket_size_totals']
        if 'input' in data:
            self.my_rank, self.key_buff2, \
            self.process_bucket_distrib_ptr1, self.process_bucket_distrib_ptr2, \
            self.min_key_val, self.max_key_val, \
            self.bucket_size_totals, self.it = data['input']
            # self.my_rank.append(my_rank)
            # self.key_buff2.append(key_buff2)
            # self.min_key_val.append(min_key_val)
            # self.max_key_val.append(max_key_val)
            key_buff1 = [0] * SIZE_OF_BUFFER
            m = 0
            for k in xrange(self.my_rank):
                for i in xrange(self.process_bucket_distrib_ptr1[k], self.process_bucket_distrib_ptr2[k] + 1):
                    m += self.bucket_size_totals[i]

            j = 0
            for i in xrange(self.process_bucket_distrib_ptr1[self.my_rank],
                            self.process_bucket_distrib_ptr2[self.my_rank] + 1):
                j += self.bucket_size_totals[i]
            # j = len(self.key_buff2)
            # self.max_key_val = 0
            # self.min_key_val = MAX_KEY

            # for i in xrange(j):
            #     self.max_key_val = max(self.max_key_val, self.key_buff2[i])
            #     self.min_key_val = min(self.min_key_val, self.key_buff2[i])

            for i in xrange(j):
                key_buff1[self.key_buff2[i] - self.min_key_val] += 1

            for i in xrange(self.max_key_val - self.min_key_val):
                key_buff1[i + 1] += key_buff1[i]

            for i in xrange(TEST_ARRAY_SIZE):
                k = self.bucket_size_totals[i + NUM_BUCKETS]
                if k in xrange(self.min_key_val, self.max_key_val + 1):
                    key_rank = key_buff1[k - self.min_key_val - 1] + m
                    self.failed = False

                    if CLASS == 'S':
                        if i <= 2:
                            if key_rank != test_rank_array[i] + self.it:
                                self.failed = True
                                print 'failed'
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)
                        else:
                            if key_rank != test_rank_array[i] - self.it:
                                self.failed = True
                                print 'failed'
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)

                    if CLASS == 'W':
                        if i < 2:
                            if key_rank != test_rank_array[i] + (self.it - 2):
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)
                        else:
                            if key_rank != test_rank_array[i] - self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)

                    if CLASS == 'A':
                        if i <= 2:
                            if key_rank != test_rank_array[i] + (self.it - 1):
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)
                        else:
                            if key_rank != test_rank_array[i] - (self.it - 1):
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)

                    if CLASS == 'B':
                        if i in [1, 2, 4]:
                            if key_rank != test_rank_array[i] + self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)
                        else:
                            if key_rank != test_rank_array[i] - self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)

                    if CLASS == 'C':
                        if i <= 2:
                            if key_rank != test_rank_array[i] + self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)
                        else:
                            if key_rank != test_rank_array[i] - self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)

                    if CLASS == 'D':
                        if i < 2:
                            if key_rank != test_rank_array[i] + self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)
                        else:
                            if key_rank != test_rank_array[i] - self.it:
                                self.failed = True
                            else:
                                self.vcounter += 1
                                #self.write('verified', 1)

                    if self.failed:
                        print "Failed partial verification:\n" \
                              "iteration %d, processor %d, test key %d" \
                              % (self.it, self.my_rank, i)

            # if self.it == MAX_ITERATIONS:
            #     self.write('output', [self.my_rank, key_buff1, self.key_buff2, j, 0, self.min_key_val, self.max_key_val])
            self.write('time', [self.my_rank, 'stop', self.it])

    # def postprocess(self):
    #     print 'verified times : ', self.vcounter

'''
Final Key Sort
'''


class FinalKeySort(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        #self._add_output('time')

    def _process(self, data):
        key_array = [0] * SIZE_OF_BUFFER
        my_rank, key_buff1, key_buff2, total_local_keys, total_lesser_keys, min_key_val, max_key_val \
            = data['input']
        for i in xrange(total_local_keys):
            key_buff1[key_buff2[i] - min_key_val] -= 1
            location = key_buff1[key_buff2[i] - min_key_val] - total_lesser_keys
            key_array[location] = key_buff2[i]
        last_local_key = 0 if total_local_keys < 1 else total_local_keys - 1
        last_local_val = key_array[last_local_key]
        first_local_val = key_array[0]

        j = 0
        for i in xrange(1, total_local_keys):
            if key_array[i - 1] > key_array[i]:
                #print key_array[i-1], key_array[i] ,i-1 ,i
                j += 1

        if j == 0:
            self.write('output', (my_rank, first_local_val, last_local_val, total_local_keys))
            #print 'sorted ok process', my_rank , min_key_val, max_key_val
        else:
            print "Processor %d:  Full_verify: number of keys out of sort: %d" \
                  % (my_rank, j)


'''
Full verification
'''


class FullVerify(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input', grouping='global')
        #self._add_output('time')
        self.first_key_val = [0] * NUM_PROCS
        self.last_key_val = [0] * NUM_PROCS
        self.total_local_keys = [0] * NUM_PROCS

    def _process(self, data):
        my_rank, first_key_val, last_key_val, total_local_keys = data['input']
        self.first_key_val[my_rank] = first_key_val
        self.last_key_val[my_rank] = last_key_val
        self.total_local_keys[my_rank] = total_local_keys

    def postprocess(self):
        j = 0
        for i in xrange(NUM_PROCS):
            if i > 0 and self.total_local_keys[i] > 0:
                if self.last_key_val[i - 1] > self.first_key_val[i]:
                    j += 1
                    #print self.last_key_val[i - 1], self.first_key_val[i]
        if j == 0:
            # passed_verification += 1
            print 'Fully Verified!'
        else:
            print 'Not Fully Verified!'


'''
Verification counter
'''

'''
class VerificationCount(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('verified', grouping='global')
        self._add_output('verification_result')
        self.counter = 0

    def _process(self, data):
        self.counter += data['verified']

    def postprocess(self):
        #print 'Verified times = %d' % self.counter
        if self.counter == 5 * MAX_ITERATIONS + 1:
            #print 'Rank passed'
            self.write('verification_result', (0, 'verified'))
        else:
            #print 'Rank failed'
            self.write('verification_result', (0, 'unverified'))
'''

'''
Timer
'''

class GlobalTimer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('time', grouping='global')
        self.start = [0.0] * NUM_PROCS
        self.elapsed = [0.0] * NUM_PROCS
        self.it = 1

    def timer_clear(self, my_rank):
        self.elapsed[my_rank] = 0.0

    def timer_start(self, my_rank):
        self.start[my_rank] = time.time()

    def timer_stop(self, my_rank):
        now = time.time()
        t = now - self.start[my_rank]
        self.elapsed[my_rank] = t

    def timer_read(self, my_rank):

        return self.elapsed[my_rank]

    def _process(self, data):
        my_rank, op, self.it = data['time']
        if op == 'start':
            self.timer_start(my_rank)
        if op == 'stop':
            self.timer_stop(my_rank)
        if op == 'clear':
            self.timer_clear(my_rank)
        if op == 'read':
            print self.timer_read(my_rank)
        if op == 'verified':
            self.verified = True

    def postprocess(self):
        os.chdir(FilePath)
        if self.it == 1:
            f = open(TimeFile, 'w')
            f.truncate()
            for my_rank in xrange(NUM_PROCS):
                f.write('%d\t%f\n' % (my_rank, self.timer_read(my_rank)))
            f.close()
        else:
            f = open(TimeFile, 'r')
            for line in f:
                my_rank, previous_time = line.strip().split('\t')
                my_rank = int(my_rank)
                self.elapsed[my_rank] += float(previous_time)
            f.close()
            f = open(TimeFile, 'w+')
            for my_rank in xrange(NUM_PROCS):
                f.write('%d\t%f\n' % (my_rank, self.timer_read(my_rank)))
            f.close()
        if self.it == MAX_ITERATIONS:
            maxtime = 0.0 
            for my_rank in xrange(NUM_PROCS):
                # maxtime = max(maxtime, self.elapsed[my_rank])
                maxtime = maxtime + self.elapsed[my_rank] / NUM_PROCS
            mop = (float)(MAX_ITERATIONS * TOTAL_KEYS * MIN_PROCS / maxtime / 1000000.0)
            self.log('Benchmark IS')
            self.log('Size : %d (Class : %s)' % (TOTAL_KEYS * MIN_PROCS, CLASS))
            self.log('Iterations : %d' % MAX_ITERATIONS)
            self.log('Number of processes : %d' % NUM_PROCS)
            self.log('Time in seconds : %f' % maxtime)
            self.log('Mop/s total : %f' % mop)
            self.log('Mop/s/process : %f' % (mop / float(NUM_PROCS)))


'''
Set up workflow graph
'''

Initializer = Init()
Initializer.numprocesses = 1
BucketSetter = BucketSet()
BucketSetter.numprocesses = nprocs
BucketSizeAggregator = BucketSizeAggregate()
BucketSizeAggregator.numprocesses = 1
BucketRedistributeSender = BucketRedistributeSend()
BucketRedistributeSender.numprocesses = nprocs
BucketRedistributeRecver = BucketRedistributeRecv()
BucketRedistributeRecver.numprocesses = nprocs + 1
PartialVerifier = PartialVerify()
PartialVerifier.numprocesses = nprocs
# FinalKeySorter = FinalKeySort()
# FinalKeySorter.numprocesses = nprocs
# FullVerifier = FullVerify()
# FullVerifier.numprocesses = 1
GlobalTimer = GlobalTimer()
GlobalTimer.numprocesses = 1

graph = WorkflowGraph()
graph.connect(Initializer, 'output', BucketSetter, 'input')
graph.connect(BucketSetter, 'bucket_size', BucketSizeAggregator, 'bucket_size')
graph.connect(BucketSetter, 'output', BucketRedistributeSender, 'input')
graph.connect(BucketSizeAggregator, 'bucket_size_totals', BucketRedistributeSender, 'bucket_size_totals')
graph.connect(BucketRedistributeSender, 'output', BucketRedistributeRecver, 'input')
graph.connect(BucketRedistributeRecver, 'output', PartialVerifier, 'input')
# graph.connect(PartialVerifier, 'output', FinalKeySorter, 'input')
# graph.connect(FinalKeySorter, 'output', FullVerifier, 'input')
graph.connect(BucketSetter, 'time', GlobalTimer, 'time')
graph.connect(PartialVerifier, 'time', GlobalTimer, 'time')

#dispel4py multi IS.py -n 8 -d '{"Init" : [{"input" : 1}]}'