import os, sys

'''
Variables definition
'''

NUM_PROCS = 1 << 5
CLASS = 'B'
output_path = '/exports/work/inf_dir/src/dispel4py/Zheming'
filename = 'array_' + CLASS + '_' + str(NUM_PROCS)

os.chdir(output_path)
f = open(filename, 'w')
f.truncate()

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

'''
Random number generator
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
Seed generator
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
Sequence generator
'''


def create_seq(seed, a):
    k = MAX_KEY / 4
    # sequence = []

    seed = [seed]
    a = [a]

    for i in xrange(NUM_KEYS):
        x = randlc(seed, a)
        x += randlc(seed, a)
        x += randlc(seed, a)
        x += randlc(seed, a)
        x = int(k * x)
        f.write('%d ' % x)

    f.write('\n')
        # sequence.append(int(k * x))

    # return sequence

'''
Main
'''

for i in xrange(NUM_PROCS):
    f.write('%d\t' % i)
    # key_array = create_seq(find_my_seed(i,
    #                                     NUM_PROCS,
    #                                     4 * long(TOTAL_KEYS) * MIN_PROCS,
    #                                     314159265.00,
    #                                     1220703125.00),
    #                         1220703125.00)
    create_seq(find_my_seed(i,
                            NUM_PROCS,
                            4 * long(TOTAL_KEYS) * MIN_PROCS,
                            314159265.00,
                            1220703125.00), 1220703125.00)
    f.flush()
    os.fsync(f.fileno())

f.close()