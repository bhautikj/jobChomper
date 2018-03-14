import time
import logging

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
    logging.error("Attempting to run work function in abstract base class.")
    raise ValueError("[Node] can't run node base class")


class STARTNODE(Node):
  def __init__(self):
    super().__init__()
    self.nodeName = "STARTNODE"

  def work(self, params):
    return True
      
def nodeExists(nodeName):
  for clss in Node.__subclasses__():
    if clss.__name__ == nodeName:
      return True
      
  logging.debug("Count not find node: " + nodeName)
  return False
  
def createNodeByName(nodeName):
  for clss in Node.__subclasses__():
    if clss.__name__ == nodeName:
      x = clss()
      x.nodeName = nodeName
      logging.debug("Created node of type " + nodeName)
      return x

  logging.error("Can't find node of type " + nodeName)
  raise ValueError("[Node] can't find node of type: " + nodeName)