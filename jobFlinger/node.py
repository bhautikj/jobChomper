import time

JOBPROGRESSKEY = "progress"
PENDINGKEY = "pending"
INPROGRESSKEY = "inProgress"
FAILEDKEY = "failed"
DONEKEY = "done"
TIMESTARTKEY = "timeStart"
TIMEENDKEY = "timeEnd"

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
    success = False
    
    timeStart = 0
    timeEnd = 0

    for i in range(self.maxRetries):
      if success == False:
        success = self.work(params)
      
      if success == True:
        params[TIMESTARTKEY] = currentMilliTime()
        params[JOBPROGRESSKEY][self.nodeName] = DONEKEY
        params[TIMEENDKEY] = currentMilliTime()
        params.writeJournal()
    
    if success == False:
      params[JOBPROGRESSKEY][self.nodeName] = FAILEDKEY
      params.writeJournal()
    
    return success


class StartNode(Node):
  def __init__(self):
    super().__init__()
    self.nodeName = "StartNode"

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