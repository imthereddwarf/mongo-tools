# -*- coding: utf-8 -*-

import dateutil.parser
import os
import pytz
import stat
import sys
import re

# flexible date parse; supply default timezone of utc
def get_time(t):
    t = dateutil.parser.parse(t)
    if not t.tzinfo:
        t = pytz.utc.localize(t)
    epoch = dateutil.parser.parse("1970-01-01T00:00:00Z")
    return (t - epoch).total_seconds()

# read lines fron a file, reporting progrss
def progress(file):
    
    s =  os.fstat(file.fileno())
    if stat.S_ISREG(s.st_mode):
        file_size = s.st_size
    else:
        file_size = None
    pos = 0
            
    # iterate through file
    for i, line in enumerate(file):

        # show progress
        pos += len(line)
        if i % 10000 == 0 and not file.isatty():
            if file_size:
                print("read %.1f%%\r" % (100.0 * pos / file_size), file=sys.stderr, end="")
            else:
                print("line %d\r" % i, file=sys.stderr, end="")
    
        # yield result
        yield i, line


def simplify(func, lt, rt, repl):
    func = func.strip()
    simple = ''
    t = 0
    for c in func:
        if c==lt:
            if t==0:
                simple += repl
            t += 1
        elif c==rt:
            t -= 1
        elif t==0:
            simple += c
    return simple

# sanitize function names, removing anything not suitable for csv headers
# xxx not sure all this function belongs here
def csv_sanitize(f):
    f = f.replace('(anonymous namespace)::', '')
    f = re.sub('{[^}]*}::', '', f)
    f = simplify(f, '<', '>', '<...>')
    f = simplify(f, '(', ')', '')
    f = simplify(f, '[', ']', '')
    f = f.replace(' const', '')
    return f


# call tree
# each node keeps a time-indexed map of weights for that node,
# and a map of child nodes indexed by function name
class Node:

    def __init__(self, root=None):
        self.root = root if root else self
        self.children = {}           # map from function name to child node
        self.sorted_children = []    # children sorted by total weight
        self.weights = {}            # time-indexed map of weights for this node of call tree
        self.hot = {}                # of the weights, how much is hot
        self.total = 0               # total weight for this node

    # pour an accumulated stack sample into the call tree
    def add(self, t, stack, weight, hot):
        if not t in self.weights:
            self.weights[t] = 0
            self.hot[t] = 0
        self.weights[t] += weight
        self.hot[t] += hot
        self.total += weight
        if stack:
            if not stack[0] in self.children:
                self.children[stack[0]] = Node(self.root)
            self.children[stack[0]].add(t, stack[1:], weight, hot)

    # emit the tree as a csv file using csv header metadata to cause t2 to format as a tree
    def get_csv_captions(self, style, level=0):
        pfx = level * "·"
        if not self.sorted_children:
            # include func in sort key to make sort order predictable for testing
            self.sorted_children = sorted(self.children.keys(),
                                          key = lambda func: (-self.children[func].total, func))
        for func in self.sorted_children:
            child = self.children[func]
            if style=='percent':
                for t in child.weights:
                    child.weights[t] = float(child.weights[t]) / self.root.weights[t]
                if not any(child.weights[t] >= self.root.opt.prune for t in child.weights):
                    child.weights = None
                    continue
                meta = [
                    'units=%',
                    'scale=0.01',
                    'yMax=100',
                    'section=CPU profile'
                ]
            elif style=='threads':
                meta = [
                    'rate=true',                # taking derivative of cumulative time
                    'units=threads',            # gives units of threads
                    'scale=1000000',            # times are in us
                    'logMin=0.1',               # low cutoff for log chart is 0.1 threads
                    'section=Thread profile',
                    'scaleGroup=thread profile'
                ]
            meta.append('name=%s' % func)
            meta.append('treeLevel=%d' % level)
            # key is just order as a string so that order is
            # preserved when it goes through the key -> value
            # hash map in t2. Displayed name is assigned using name= metadata.
            if self.root.opt.temperature:
                meta.append('chart=tree-chart-%d' % self.root.order)
                for n, c in [('cold',0), ('hot',1), ('total',5)]: # blue, red, gray - see charts.ts
                    yield "%06d#distinctName=%s;color=%d;%s" % (
                        self.root.order, n, c, ';'.join(meta))
                    self.root.order += 1
            else:
                yield "%06d#%s" % (self.root.order, ';'.join(meta))
                self.root.order += 1
            for caption in child.get_csv_captions(style, level+1):
                yield caption


    # get the weights for each node
    # must return same number and order as captions
    def get_weights(self, t):
        for func in self.sorted_children:
            child = self.children[func]
            if child.weights:
                if self.root.opt.temperature:
                    weight = child.weights[t] if t in child.weights else 0
                    hot = child.hot[t] if t in child.hot else 0
                    yield str(weight - hot)    # cold
                    yield str(hot)             # hot
                    yield str(weight)          # total
                else:
                    yield str(child.weights[t] if t in child.weights else 0)
                for weight in child.get_weights(t):
                    yield weight

class StacksReader:

    def __init__(self, argumentParser):
    
        self.samples = {}    # {time -> {stack -> weight}}
    
        self.stacks_by_id = {}
        self.timestamp_field = None
        self.time_field = None
        self.count_field = None
        self.stack_field = 0 # effective default format is "stack", i.e. each line is a stack
        self.stack_id_field = None
        self.format_fields = 1

        # add our args, parse, return result
        argumentParser.add_argument("--reverse", action="store_true")
        argumentParser.add_argument("--prune", type=float, default=0.01)
        argumentParser.add_argument("--temperature", action="store_true")
        self.opt = argumentParser.parse_args()
        self.opt.end = None # can be supplied by caller at a later time
    
    # add a single stack sample
    def add(self, t, stack, weight=1):
        if not stack:
            return
        if not t in self.samples:
            self.samples[t] = {}
        sample = self.samples[t]
        if not stack in sample:
            sample[stack] = 0
        sample[stack] += weight
    
    def read_stacks(self, sanitize=csv_sanitize):
        for i, line in progress(sys.stdin):
            line = line.rstrip()
            if line.startswith('#'):
                if line.startswith('#format'):
                    fs = line.split(None, 1)[1].split(';')
                    format_fields = len(fs)
                    for i, f in enumerate(fs):
                        if f=='timestamp': self.timestamp_field = i
                        elif f=='time': self.time_field = i
                        elif f=='count': self.count_field = i
                        elif f=='stack': self.stack_field = i
                        elif f=='stack-id': self.stack_id_field = i
                        else: raise Exception('unknown format field ' + f)
                elif line.startswith('#stack'):
                    fs = line.split(None, 2)
                    stack_id = fs[1]
                    self.stacks_by_id[stack_id] = tuple(fs[2].split(';'))
            else:
                fs = line.split(';')
                if len(fs) < self.format_fields:
                    raise Exception('in line %d, require at least %d fields, have %d' % \
                                    (i, format_fields, len(fs)))
                if self.stack_id_field is not None:
                    stack = self.stacks_by_id[fs[self.stack_id_field]]
                elif self.stack_field is not None:
                    stack = fs[self.stack_field:]
                else:
                    raise Exception('must specify one of stack or stack_id in format')
                if self.count_field is not None:
                    weight = float(fs[self.count_field])
                elif self.time_field is not None:
                    weight = float(fs[self.time_field])
                else:
                    weight = 1
                if self.timestamp_field is not None:
                    ts = float(fs[self.timestamp_field])
                else:
                    ts = 0
                if sanitize: stack = (sanitize(f) for f in stack)
                self.add(ts, tuple(stack), weight)
        
    # emit the csv file
    def emit_csv(self):
    
        # finish progress
        print(file=sys.stderr)
    
        # sort to ensure time is monotonic
        sample_times = sorted(self.samples.keys())
    
        # set up tree root
        root = Node()
        root.order = 0
        root.opt = self.opt

        # pour the samples into the call tree
        for i, t in enumerate(sample_times):
            print("accumulated sample %d/%d\r" % (i, len(sample_times)), file=sys.stderr, end="")
            sample = self.samples[t]
            for stack in sample:
                weight = sample[stack]
                hot = 0
                if stack[-1] and stack[-1][0]==':' and self.opt.temperature:
                    if stack[-1]==':hot':
                        hot = weight
                    stack = stack[:-1]
                if self.opt.reverse:
                    stack = ("LEAF",) + tuple(reversed(stack))
                root.add(t, stack, weight, hot)
        print(file=sys.stderr)
    
        # emit the csv file        
        style = 'threads' if self.time_field else 'percent'
        print("time," + ",".join(root.get_csv_captions(style)))
        for i, t in enumerate(sample_times):
            print("emitted sample %d/%d\r" % (i, len(sample_times)), file=sys.stderr, end="")
            print(str(t) + "," + ",".join(root.get_weights(t)))
        print(file=sys.stderr)
    
class StacksWriter:

    first = True

    # write a stack to stdout
    def stack(self, t, stack, thread = None):

        # emit format definition on first sample
        # if no t is specified assume just stacks, else include timestamp
        if self.first:
            if t is not None:
                print("#format timestamp;stack")
            self.first = False

        # emit stack
        if stack:
            stacks = ";".join(stack)
            if thread is not None:
                stacks = thread + ";" + stacks
            if t is not None:
                stacks = str(t) + ";" + stacks
            print(stacks)


