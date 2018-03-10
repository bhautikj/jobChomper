class Node(object):
  """ Node Object """
  def __init__(self):
    self.init = True
    self.runOnSuccess = []
    self.runOnFailure = []

  def run(self):
    raise ValueError("[Node] can't run node base class")
    

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