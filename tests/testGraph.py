import os
testdir = os.path.dirname(__file__)
import unittest

import jobChomper.graph
import jobChomper.node

class TestGraph(unittest.TestCase):  
  class A(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      return True

  class B(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      return True

  class C(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      return True

  class D(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      return True


  def test_init(self):
    xa = jobChomper.graph.Graph()
    self.assertTrue(xa.init)

  def test_read_nofile(self):
    xb = jobChomper.graph.Graph()
    self.assertRaises(FileNotFoundError,  xb.loadGraphFromFile, "BLAH")

  def test_read_borkedFile(self):
    xc = jobChomper.graph.Graph()
    self.assertRaises(ValueError, xc.loadGraphFromFile, os.path.join(testdir,"borkedGraph.graph"))

  def test_read_noStartNodeFile(self):
    xd = jobChomper.graph.Graph()
    self.assertRaises(ValueError, xd.loadGraphFromFile, os.path.join(testdir,"nostartGraph.graph"))

  def test_read_cycleGraphFile(self):
    xe = jobChomper.graph.Graph()
    self.assertRaises(ValueError, xe.loadGraphFromFile, os.path.join(testdir,"cycleGraph.graph"))

  def test_read_nonexistnodeGraphFile(self):
    xe = jobChomper.graph.Graph()
    self.assertRaises(ValueError, xe.loadGraphFromFile, os.path.join(testdir,"nonexistnodeGraph.graph"))
    
  def test_read_validFile(self):
    x = jobChomper.graph.Graph()
    readPass = x.loadGraphFromFile(os.path.join(testdir,"validGraph.graph"))

    # check edges have turned up as expected
    self.assertTrue((jobChomper.graph.STARTNODENAME, "A", False) in x.edges)
    self.assertTrue(("A", "B", True) in x.edges)
    self.assertTrue(("B", "C", False) in x.edges)
    self.assertTrue(("B", "D", True) in x.edges)
    
    self.assertTrue(jobChomper.graph.STARTNODENAME in x.nodeSet)
    self.assertTrue("A" in x.nodeSet)
    self.assertTrue("B" in x.nodeSet)
    self.assertTrue("C" in x.nodeSet)
    self.assertTrue("D" in x.nodeSet)

if __name__ == '__main__':
  unittest.main()