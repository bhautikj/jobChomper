import time

currentMilliTime = lambda: int(round(time.time() * 1000))

sixtyMilliseconds = 1000 * 60

class Node(object):
  """ Node Object """
  def __init__(self, timeout = sixtyMilliseconds, maxRetries = 5 ):
    self.init = True
    self.timeout = timeout
    self.maxRetries = maxRetries

  def work(self, params):
    raise ValueError("[Node] can't run node base class")

  def run(self, params):
    return self.work(params)
    

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
      return x

  raise ValueError("[Node] can't find node of type: " + nodeName)