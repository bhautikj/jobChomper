import os
testdir = os.path.dirname(__file__)
import unittest

import jobFlinger.node

class TestNode(unittest.TestCase):  
  class NODEA(jobFlinger.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      return True
      
  def test_run_node(self):
    nodeFail = jobFlinger.node.Node()
    self.assertRaises(ValueError,  nodeFail.run, {})

  def test_run_start_node(self):
    startnode = jobFlinger.node.StartNode()
    self.assertTrue(startnode.run({}))

  def test_node_exists(self):
    self.assertTrue(jobFlinger.node.nodeExists("NODEA"))
    self.assertFalse(jobFlinger.node.nodeExists("NODEB"))

  def test_create_node_by_name(self):
    class NODEC(jobFlinger.node.Node):
      def __init__(self):
        super().__init__()
  
      def work(self, params):
        return True
    
    na = jobFlinger.node.createNodeByName("NODEC")
    self.assertTrue(isinstance(na, NODEC))

  def test_fail_create_node_by_name(self):
    self.assertRaises(ValueError, jobFlinger.node.createNodeByName, "NODED")
  
  
if __name__ == '__main__':
  unittest.main()