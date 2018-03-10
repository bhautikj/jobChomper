## 
## Weirdo DAG that powers jobFlinger
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

class Graph(object):
  """ Graph Object """
  edges = set()
  init = False
  
  def __init__(self):
    self.init = True
    
    
  def loadGraphFromFile(self, filename):
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
          raise ValueError("Problem parsing: " + filename + " file has invalid triple: " + line)
        
        nodeA = spl[0]
        nodeB = spl[1]
        prevEval = False
        if spl[2].lower() == 'true':
          prevEval = True
      
        triple = (nodeA, nodeB, prevEval)
        
        self.edges.add(triple)
      
      
    return True
