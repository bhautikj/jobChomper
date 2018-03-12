import jobFlinger.graph
import jobFlinger.node
import jobFlinger.safeFileDict
import jobFlinger.directoryWrangler

import os, json

JOBSTATEFILE = "state.json"
GRAPHFILEKEY = "graphFile"
JOBPROGRESSKEY = "progress"

PENDINGKEY = "pending"
INPROGRESSKEY = "inProgress"
FAILEDKEY = "failed"
DONEKEY = "done"


class RunGraph(object):
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
    if JOBPROGRESSKEY not in self.state.keys():
      self.state[JOBPROGRESSKEY] = {}
      for node in self.graph.nodeSet:
        self.state[JOBPROGRESSKEY][node] = { "status" : PENDINGKEY,
                                             "numAttempts" : 0,
                                             "runs": []}
                                             
    print(self.state[JOBPROGRESSKEY])

  def run(self):
    print("x")