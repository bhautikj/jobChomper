import unittest

import jobFlinger.graph

class TestLoadGraph(unittest.TestCase):
  def test_init(self):
    x = jobFlinger.graph.Graph()
    self.assertTrue(x.init)
    
if __name__ == '__main__':
  unittest.main()