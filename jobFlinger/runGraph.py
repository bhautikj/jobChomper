import jobFlinger.graph
import jobFlinger.node
import jobFlinger.safeFileDict
import jobFlinger.directoryWrangler

import os, json
#from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import concurrent.futures

POOLSIZE = 5

JOBSTATEFILE = "state.json"
GRAPHFILEKEY = "graphFile"

def runNode(namedNode, state):
  node = jobFlinger.node.createNodeByName(namedNode)
  node.run(state)

class RunGraph(object):
  poolExecutors = concurrent.futures.ThreadPoolExecutor(POOLSIZE)
  
  """ RunGraph Object """
  def __init__(self, directoryWrangler, jobID):
    self.directoryWrangler = directoryWrangler
    self.jobID = jobID
    self.jobDir = self.directoryWrangler.getVar(self.jobID)
    self.loadState()
  
  def loadState(self):
    statefile = os.path.join(self.jobDir, JOBSTATEFILE)
    if not os.path.exists(statefile):
      raise ValueError("[RunGraph] state file for " + self.jobID + " missing")
      
    with open(statefile) as statedict:
      self.state = jobFlinger.safeFileDict.SafeFileDict(json.loads(statedict.read()))
      self.state.enableJournal(statefile)

    # get graph from state
    self.createGraph()
    
    # check job progress
    self.initJobProgress()
        
  def createGraph(self):
    graphfile = os.path.join(self.jobDir, self.state[GRAPHFILEKEY])
    self.graph = jobFlinger.graph.Graph()
    self.graph.loadGraphFromFile(graphfile)
    
  def initJobProgress(self):
    if jobFlinger.node.JOBPROGRESSKEY not in self.state.keys():
      self.state[jobFlinger.node.JOBPROGRESSKEY] = {}
      for node in self.graph.nodeSet:
        self.state[jobFlinger.node.JOBPROGRESSKEY][node] = { "status" : jobFlinger.node.PENDINGKEY }
      self.state.writeJournal()

  def needsToRun(self, nodeName, rerunFailed):
    if rerunFailed:
      testSet = [jobFlinger.node.PENDINGKEY, jobFlinger.node.INPROGRESSKEY, jobFlinger.node.FAILEDKEY]
    else:
      testSet = [jobFlinger.node.PENDINGKEY, jobFlinger.node.INPROGRESSKEY]
    return self.state[jobFlinger.node.JOBPROGRESSKEY][nodeName]["status"] in testSet

  def isDone(self, nodeName, rerunFailed):
    if rerunFailed:
      testSet = [jobFlinger.node.DONEKEY]
    else:
      testSet = [jobFlinger.node.FAILEDKEY, jobFlinger.node.DONEKEY]
    return self.state[jobFlinger.node.JOBPROGRESSKEY][nodeName]["status"] in testSet

  def isFailed(self, nodeName):
    return self.state[jobFlinger.node.JOBPROGRESSKEY][nodeName]["status"] in [jobFlinger.node.FAILEDKEY]
    
  def graphWalk(self, rerunFailed):
    # traverse graph & create sets - done (jobFlinger.node.FAILEDKEY, jobFlinger.node.DONEKEY) and 
    # pending (jobFlinger.node.INPROGRESSKEY, jobFlinger.node.PENDINGKEY). In-progress jobs get 
    # restarted on a reload; can force a rerun of failed nodes if
    # param rerunFailed == True
    runQueue = set()
        
    # start with STARTNODENAME
    if self.needsToRun(jobFlinger.graph.STARTNODENAME, rerunFailed):
      runQueue.add(jobFlinger.graph.STARTNODENAME)
      return runQueue
    
    checkNodes = set([jobFlinger.graph.STARTNODENAME])
    
    while len(checkNodes) != 0:
      checkCopy = set(checkNodes)
      #print("\nChecking nodes:", checkNodes, " run queue:", runQueue)
      for node in checkCopy:
        checkNodes.remove(node)
        if self.isDone(node, rerunFailed):
          if self.isFailed(node):
            # only run on fail children
            for child in self.graph.runDict[node][jobFlinger.graph.RUNONFAIL]:
              #print("\nNode failed, adding child ", child)
              checkNodes.add(child)
          else:
            # run all children
            for child in self.graph.runDict[node][jobFlinger.graph.RUNONFAIL]:
              #print("\nNode passed, adding on fail child ", child)
              checkNodes.add(child)

            for child in self.graph.runDict[node][jobFlinger.graph.RUNONLYONPASS]:
              #print("\nNode passed, adding on pass child ", child)
              checkNodes.add(child)
        else:
          runQueue.add(node)
    
    return runQueue
    
  def graphRun(self, rerunFailed):
    # state is self.state
    runQueue = self.graphWalk(rerunFailed)
    while len(runQueue) != 0:
      futures = []
      for nodeName in runQueue:
        futures.append(self.poolExecutors.submit(runNode, nodeName, self.state))
      results = concurrent.futures.wait(futures)
    
    