import jobChomper.graph
import jobChomper.node
import jobChomper.safeFileDict
import jobChomper.directoryWrangler

import os, json, shutil
import concurrent.futures

POOLSIZE = 5

JOBSTATEFILE = "state.json"
GRAPHFILEKEY = "graphFile"
NUMTRIESKEY = "numTries"

import inspect

def runNode(namedNode, state):

  if not jobChomper.node.nodeExists(namedNode):
    print("NO NODE: ", namedNode)
    state[jobChomper.node.JOBPROGRESSKEY][namedNode]["status"]  = jobChomper.node.FAILEDKEY
    state.writeJournal()
    
  node = jobChomper.node.createNodeByName(namedNode)

  results = {}
  
  success = False

  timeStart = 0
  timeEnd = 0

  for i in range(node.maxRetries):
    if success == False:
      state[jobChomper.node.JOBPROGRESSKEY][namedNode][jobChomper.node.TIMESTARTKEY] = jobChomper.node.currentMilliTime()
      success = node.work(state)
      state[jobChomper.node.JOBPROGRESSKEY][namedNode][jobChomper.node.TIMEENDKEY] = jobChomper.node.currentMilliTime()
      

    if success == True:
      state[jobChomper.node.JOBPROGRESSKEY][namedNode]["status"] = jobChomper.node.DONEKEY
      state.writeJournal()
      break

  state[jobChomper.node.JOBPROGRESSKEY][namedNode][NUMTRIESKEY] = str(i+1)    

  if success == False:
    state[jobChomper.node.JOBPROGRESSKEY][namedNode]["status"]  = jobChomper.node.FAILEDKEY
    state.writeJournal()

  return success
  
class RunGraph(object):
  poolExecutors = concurrent.futures.ThreadPoolExecutor(POOLSIZE)
  
  """ RunGraph Object """
  def __init__(self, directoryWrangler, jobID):
    self.directoryWrangler = directoryWrangler
    self.jobID = jobID
    self.jobDir = self.directoryWrangler.getVar(self.jobID)
    #self.loadState()
  
  def initFromGraph(self, graphFile):
    if not os.path.isfile(graphFile):
      raise ValueError("[RunGraph] no such graph file " + graphFile)

    graphBase = os.path.basename(graphFile)  
    jobDirectory = self.directoryWrangler.getVar(self.jobID)
    shutil.copyfile(graphFile, os.path.join(jobDirectory, graphBase))
    
    statefile = os.path.join(self.jobDir, JOBSTATEFILE)
    tmpState = jobChomper.safeFileDict.SafeFileDict({jobChomper.runGraph.GRAPHFILEKEY : graphBase})
    tmpState.enableJournal(statefile)
    tmpState.writeJournal()
    
    self.loadState()
  
  def loadState(self):
    statefile = os.path.join(self.jobDir, JOBSTATEFILE)
    if not os.path.exists(statefile):
      raise ValueError("[RunGraph] state file for " + self.jobID + " missing")
      
    with open(statefile) as statedict:
      self.state = jobChomper.safeFileDict.SafeFileDict(json.loads(statedict.read()))
      self.state.enableJournal(statefile)

    # get graph from state
    self.createGraph()
    
    # check job progress
    self.initJobProgress()
        
  def createGraph(self):
    graphfile = os.path.join(self.jobDir, self.state[GRAPHFILEKEY])
    self.graph = jobChomper.graph.Graph()
    self.graph.loadGraphFromFile(graphfile)
    
  def initJobProgress(self):
    if jobChomper.node.JOBPROGRESSKEY not in self.state.keys():
      self.state[jobChomper.node.JOBPROGRESSKEY] = {}
      for node in self.graph.nodeSet:
        self.state[jobChomper.node.JOBPROGRESSKEY][node] = {"status" : jobChomper.node.PENDINGKEY}
      self.state.writeJournal()

  def needsToRun(self, nodeName, rerunFailed):
    if rerunFailed:
      testSet = [jobChomper.node.PENDINGKEY, jobChomper.node.INPROGRESSKEY, jobChomper.node.FAILEDKEY]
    else:
      testSet = [jobChomper.node.PENDINGKEY, jobChomper.node.INPROGRESSKEY]
    return self.state[jobChomper.node.JOBPROGRESSKEY][nodeName]["status"] in testSet

  def isDone(self, nodeName, rerunFailed):
    if rerunFailed:
      testSet = [jobChomper.node.DONEKEY]
    else:
      testSet = [jobChomper.node.FAILEDKEY, jobChomper.node.DONEKEY]
    return self.state[jobChomper.node.JOBPROGRESSKEY][nodeName]["status"] in testSet

  def isFailed(self, nodeName):
    return self.state[jobChomper.node.JOBPROGRESSKEY][nodeName]["status"] in [jobChomper.node.FAILEDKEY]
    
  def graphWalk(self, rerunFailed):
    # traverse graph & create sets - done (jobChomper.node.FAILEDKEY, jobChomper.node.DONEKEY) and
    # pending (jobChomper.node.INPROGRESSKEY, jobChomper.node.PENDINGKEY). In-progress jobs get
    # restarted on a reload; can force a rerun of failed nodes if
    # param rerunFailed == True
    runQueue = set()
        
    # start with STARTNODENAME
    if self.needsToRun(jobChomper.graph.STARTNODENAME, rerunFailed):
      runQueue.add(jobChomper.graph.STARTNODENAME)
      return runQueue
    
    checkNodes = set([jobChomper.graph.STARTNODENAME])
    
    while len(checkNodes) != 0:
      checkCopy = set(checkNodes)
      #print("\nChecking nodes:", checkNodes, " run queue:", runQueue)
      for node in checkCopy:
        checkNodes.remove(node)
        if self.isDone(node, rerunFailed):
          if self.isFailed(node):
            # only run on fail children
            for child in self.graph.runDict[node][jobChomper.graph.RUNONFAIL]:
              #print("\nNode failed, adding child ", child)
              checkNodes.add(child)
          else:
            # run all children
            for child in self.graph.runDict[node][jobChomper.graph.RUNONFAIL]:
              #print("\nNode passed, adding on fail child ", child)
              checkNodes.add(child)

            for child in self.graph.runDict[node][jobChomper.graph.RUNONLYONPASS]:
              #print("\nNode passed, adding on pass child ", child)
              checkNodes.add(child)
        else:
          runQueue.add(node)
    
    return runQueue
    
  def graphRun(self, rerunFailed):
    # state is self.state
    runQueue = list(self.graphWalk(rerunFailed))
    while len(runQueue) != 0:
      futures = []
      for nodeName in runQueue:
        futures.append(self.poolExecutors.submit(runNode, nodeName, self.state))
      done, not_done = concurrent.futures.wait(futures)
      
      # iterate over futures - it'll throw exceptions from the runs as needed
      for job in done:
        result = job.result()
      
      # TODO: more sophisticated handling of the not_done set to mark those jobs
      # as failed.            
      runQueue = self.graphWalk(rerunFailed)
    
    