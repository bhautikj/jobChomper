import jobFlinger.graph
import jobFlinger.node
import jobFlinger.safeFileDict
import jobFlinger.directoryWrangler

import os

JOBSTATEFILE = "state.json"

#
# when we run a graph, we create a working directory in var OR it gets passsed in (in the case of failed jobs)
# return that directory so data can be put into it before execution
#
class RunGraph(object):
  """ RunGraph Object """
  def __init__(self, graphObject, workingDir):
    self.graph = graphObject
    # build state object
    self.state = jobFlinger.safeFileDict.SafeFileDict()
    stateFile = os.path.join(workingDir, 'state.json')