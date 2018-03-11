## 
## Weirdo Tree Graph that powers jobFlinger
## --
##
## Assertions: 
##   * DAG is made up of named edges
##   * Each edge is a triple (A, B, NEEDSPREVIOUSTOPASS)
##     A, B are the named nodes
##     B will execute after A has evaluated
##     NEEDSPREVIOUSTOPASS is True or False; if it is True then A _must_ evaluate as True for B to run
##   * There's a special node called STARTNODE from where execution starts
##   * Comment lines in graph file start with #
##   * Elements in graph lines separated by ',' - for example:
##     A, B, True
##
import jobFlinger.node

STARTNODENAME = "STARTNODE"

def findCycle(graph):
  todo = set(graph.keys())
  while todo:
    node = todo.pop()
    stack = [node]
    while stack:
      top = stack[-1]
      for node in graph[top]:
        if node in stack:
          return stack[stack.index(node):]
        if node in todo:
          stack.append(node)
          todo.remove(node)
          break
      else:
        node = stack.pop()
  return None


class Graph(object):
  """ Graph Object """
  
  def __init__(self):
    self.init = True
    self.edges = set()
    self.runDict = {}

  def buildRunDict(self):
    self.runDict = {}
    for edge in self.edges:
      nodeA = edge[0]
      nodeB = edge[1]
      if nodeA not in self.runDict.keys():
        self.runDict[nodeA] = []
      self.runDict[nodeA].append(nodeB)
          
  def findCycles(self):
    return findCycle(self.runDict)    

  def checkEdgeNodesValid(self):
    for edge in self.edges:
      nodeA = edge[0]
      nodeB = edge[1]
      
      if nodeA == STARTNODENAME:
        continue

      if not jobFlinger.node.nodeExists(nodeA):
        raise ValueError("[Graph] no such node as: " + nodeA)

      if not jobFlinger.node.nodeExists(nodeB):
        raise ValueError("[Graph] no such node as: " + nodeB)

  
  def loadGraphFromFile(self, filename):
    foundStart = False
    
    with open(filename) as graphBody:
      data = graphBody.read()
      for line in data.split('\n'):
        line = line.strip()
        # Empty line
        if line == '':
          continue
          
        # Comment line
        if line[0] == '#':
          continue
        spl = line.split(',')
        
        # Not a triple
        if len(spl) != 3:
          raise ValueError("[Graph] Problem parsing: " + filename + " file has invalid triple: " + line)
        
        nodeA = spl[0].strip()
        nodeB = spl[1].strip()
        prevEval = False
        if spl[2].lower().strip() == 'true':
          prevEval = True
      
        if nodeA == STARTNODENAME:
          if foundStart == True:
            raise ValueError("[Graph] Problem parsing: " + filename + " start node defined again: " + line)
          else:
            foundStart = True
      
        triple = (nodeA, nodeB, prevEval)
        
        self.edges.add(triple)

    if foundStart == False:   
      raise ValueError("[Graph] Problem parsing: " + filename + " cound not find " + STARTNODENAME)
      
    self.buildRunDict()
    
    cycles = self.findCycles()
    if cycles != None:
      raise ValueError("[Graph] Problem parsing: " + filename + " cycle detected:" + str(cycles))
      
    self.checkEdgeNodesValid()      
      
    