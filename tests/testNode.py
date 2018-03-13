import os
testdir = os.path.dirname(__file__)
import unittest

import jobChomper.node
import jobChomper.safeFileDict

class TestNode(unittest.TestCase):  
  class NODEA(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      return True
      
  def test_run_node(self):
    nodeFail = jobChomper.node.Node()
    stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                 jobChomper.node.JOBPROGRESSKEY: {
                    jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.PENDINGKEY},
                    "NODEA" : { "status" : jobChomper.node.PENDINGKEY}
                  }}
    self.assertRaises(ValueError, nodeFail.work, jobChomper.safeFileDict.SafeFileDict(stateDict))

  def test_run_start_node(self):
    startnode = jobChomper.node.STARTNODE()
    stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                 jobChomper.node.JOBPROGRESSKEY: {
                    jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.PENDINGKEY},
                    "NODEA" : { "status" : jobChomper.node.PENDINGKEY}
                  }}
    self.assertTrue(startnode.work(jobChomper.safeFileDict.SafeFileDict(stateDict)))

  def test_node_exists(self):
    self.assertTrue(jobChomper.node.nodeExists("NODEA"))
    self.assertFalse(jobChomper.node.nodeExists("NODEB"))

  def test_create_node_by_name(self):
    class NODEC(jobChomper.node.Node):
      def __init__(self):
        super().__init__()
  
      def work(self, params):
        return True
    
    na = jobChomper.node.createNodeByName("NODEC")
    self.assertTrue(isinstance(na, NODEC))

  def test_fail_create_node_by_name(self):
    self.assertRaises(ValueError, jobChomper.node.createNodeByName, "NODED")
  
  
if __name__ == '__main__':
  unittest.main()
