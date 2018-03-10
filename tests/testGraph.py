import os
testdir = os.path.dirname(__file__)
import unittest

import jobFlinger.graph


class TestGraph(unittest.TestCase):
  def test_init(self):
    xa = jobFlinger.graph.Graph()
    self.assertTrue(xa.init)

  def test_read_nofile(self):
    xb = jobFlinger.graph.Graph()
    self.assertRaises(FileNotFoundError,  xb.loadGraphFromFile, "BLAH")

  def test_read_borkedFile(self):
    xc = jobFlinger.graph.Graph()
    self.assertRaises(ValueError, xc.loadGraphFromFile, os.path.join(testdir,"borkedGraph.graph"))

  def test_read_noStartNodeFile(self):
    xd = jobFlinger.graph.Graph()
    self.assertRaises(ValueError, xd.loadGraphFromFile, os.path.join(testdir,"nostartGraph.graph"))

  def test_read_cycleGraphFile(self):
    xe = jobFlinger.graph.Graph()
    self.assertRaises(ValueError, xe.loadGraphFromFile, os.path.join(testdir,"cycleGraph.graph"))
    
  def test_read_validFile(self):
    x = jobFlinger.graph.Graph()
    readPass = x.loadGraphFromFile(os.path.join(testdir,"validGraph.graph"))

    # check edges have turned up as expected
    self.assertTrue((jobFlinger.graph.STARTNODENAME, "A", False) in x.edges)
    self.assertTrue(("A", "B", True) in x.edges)
    self.assertTrue(("B", "C", False) in x.edges)
    self.assertTrue(("B", "D", True) in x.edges)


if __name__ == '__main__':
  unittest.main()