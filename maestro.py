import json
import time
from subprocess import Popen, PIPE

_all_ = ["Controller", "CyclicDAGError"]

def _check_cyclic(g):
    """Returns true if the directed graph g has a cycle.
    Dag must be represented as a dictionary mapping vertices
    to iterables of neighbouring vertices. For example:

    >>> _check_cyclic({ 0 : (2,), 1 : (0,), 2 : (1,)})
    True
    >>> _check_cyclic({ 0 : (), 1 : (0,), 2 : (1,)})
    False

    Source: http://codereview.stackexchange.com/questions/86021
    /check-if-a-directed-graph-contains-a-cycle
    """
    path = set()
    def visit(vertex):
        path.add(vertex)
        for neighbour in g.get(vertex, ()):
            if neighbour in path or visit(neighbour):
                return True
        path.remove(vertex)
        return False

    return any(visit(v) for v in g)

def _topological_search(g):
    """"Execute a topological search on a Directed Acyclic Graph (DAG)

    Input consist of a dictionary mapping nodes to it`s dependencys
    represented by any Iterable. If the node have no dependency, an
    empty iterable is expected

    If the Graph contains Cycles, a ValueError Exception will be
    raised

    The topological search implemented here is Kahn`s Algorithm
    https://en.wikipedia.org/wiki/Topological_sorting
    """
    if len(g) == 0:
        return
    # Copy the input and transform dependency object into Sets
    g = { k: set(v) for k, v in g.items()}

    # Ignoring self dependencies
    for k, v in g.items():
        v.discard(k)

    L = []
    S = set(x for x in g.keys() if len(g[x]) == 0)

    while len(S) > 0:
        n = S.pop()
        L.append(n)
        print("n = {}".format(n))
        for m in g.keys():
            if n in g[m]:
                g[m].remove(n)
                if len(g[m]) == 0:
                    S.add(m)
    #If the Graph still have edges, then it`s not a DAG
    if max([len(v) for k, v in g.items()]) > 0:
        raise CyclicDAGError(g, "The input graph is cyclic. Check your Job Description JSON file")
    else:
        return L

def _buildDAG(jobdesc):
    """Build a DAG representation (A dictionary mapping vertices
    to iterables of neighbouring vertices) from a Job Description
    Dictionary
    """
    ret = {}
    for id, job in jobdesc["dag"].items():
        ret[id] = set(job["require"])
    if _check_cyclic(ret):
        raise CyclicDAGError(ret, "The input graph is cyclic. Check your Job Description JSON file")
    return ret

class CyclicDAGError(ValueError):
    """"Exception raised when a DAG is cyclic

    Attributes:
        g -- The graph describing the wannabe DAG
        message -- Explanation of the error
    """

    def __init__(self,g,message):
        self.g = g
        self.message = message

class Controller(object):
    """Main class of Maestro Job Controller"""
    def __init__(self,jobpath):
        with open(jobpath,"r") as jobfile:
            self._jobdesc = json.load(jobfile)
            self._dag = _buildDAG(self._jobdesc)
            self._queue = set(self._dag.keys())
            self._current = {}
            self._finished = set()
            self._fail = set()
            self._ignored = set()
    def _remove_failure(self,n):
        """Remove all dependent jobs on queue
        after a failure
        """
        dependent = set(k for k, v in self._dag.items() if n in v)
        self._queue = set(x for x in self._queue if x not in dependent)
        self._ignored = self._ignored | dependent
        for d in dependent:
            self._remove_failure(d)

    def start(self,workers=1,wait=60):
        working = 0
        if workers < 1:
            print("Not job can be done with no workers")
            return
        while self._current or self._queue:
            #Check if any of the current workers ended
            finished = []
            for n, w in self._current.items():
                retcode = w.poll()
                if retcode is not None:
                    working = working - 1
                    finished.append(n)
                    if retcode == 0:
                        print("Job finished with SUCCESS - JOBID = {}".format(n))
                        self._finished.add(n)
                    else:
                        print("Job finished with FAILURE - JOBID = {}".format(n))
                        self._fail.add(n)
                        self._remove_failure(n)
            for n in finished:
                del self._current[n]
            next_jobs = set(m for m in self._queue if self._dag[m].issubset(self._finished))
            #Start Next Job on Queue, if any]
            while len(next_jobs) > 0 and working < workers:
                next = next_jobs.pop()
                self._queue.remove(next)
                self._current[next] = Popen(self._jobdesc["dag"][next]["startcmd"], stdout=PIPE, stderr=PIPE)
                print("Starting next job on Worker - JOBID = {}".format(next))
                working = working + 1
            time.sleep(1)
        print("FINISHED = {}".format(self._finished))
        print("FAILED = {}".format(self._fail))
        print("IGNORED = {}".format(self._ignored))
        print("---")



