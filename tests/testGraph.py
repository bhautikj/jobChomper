import os
testdir = os.path.dirname(__file__)
import unittest

import jobFlinger.graph


class TestGraph(unittest.TestCase):
  def test_init(self):
    x = jobFlinger.graph.Graph()
    self.assertTrue(x.init)

  def test_read_nofile(self):
    x = jobFlinger.graph.Graph()
    self.assertRaises(FileNotFoundError,  x.loadGraphFromFile, "BLAH")

  def test_read_borkedFile(self):
    x = jobFlinger.graph.Graph()
    self.assertRaises(ValueError, x.loadGraphFromFile, os.path.join(testdir,"borkedGraph.graph"))
    
  def test_read_validFile(self):
    x = jobFlinger.graph.Graph()
    readPass = x.loadGraphFromFile(os.path.join(testdir,"validGraph.graph"))
    self.assertTrue(readPass)

  
if __name__ == '__main__':
  unittest.main()