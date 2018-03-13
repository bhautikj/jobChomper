import time

JOBPROGRESSKEY = "progress"
PENDINGKEY = "pending"
INPROGRESSKEY = "inProgress"
FAILEDKEY = "failed"
DONEKEY = "done"

currentMilliTime = lambda: int(round(time.time() * 1000))
sixtySeconds = 1000 * 60

class Node(object):
  """ Node Object """
  def __init__(self, timeout = sixtySeconds, maxRetries = 5 ):
    self.init = True
    self.timeout = timeout
    self.maxRetries = maxRetries

  def work(self, params):
    raise ValueError("[Node] can't run node base class")

  def run(self, params):
    numRuns = 0
    success = False
    
    #        self.state[JOBPROGRESSKEY][node] = { "status" : PENDINGKEY }
    
    testRun = False
    while success == False and numRuns < self.maxRetries:
      try:
        testRun = self.work(params)
        success = True
      except:
        numRuns += 1
        
    #if success == False:
    #  params[]
    return testRun
      

class StartNode(Node):
  def __init__(self):
    super().__init__()

  def work(self, params):
    return True
      
def nodeExists(nodeName):
  for clss in Node.__subclasses__():
    if clss.__name__ == nodeName:
      return True
  return False
  
def createNodeByName(nodeName):
  for clss in Node.__subclasses__():
    if clss.__name__ == nodeName:
      x = clss()
      x.nodeName = nodeName
      return x

  raise ValueError("[Node] can't find node of type: " + nodeName)