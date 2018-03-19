import os
testdir = os.path.dirname(__file__)
import unittest
import shutil
import uuid
import json
import random
import time

import jobChomper.chomper

class TestChomper(unittest.TestCase):
  class cA(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      time.sleep(0.5)
      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("cA")
      return True

  class cB(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      time.sleep(0.5)
      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("cB")
      return True

  class cC(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      time.sleep(0.5)
      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("cC")
      return True
  
  def test_chomperBasic(self):
    testBase = os.path.join(testdir, str(uuid.uuid4()))
    try:
      chomper = jobChomper.chomper.Chomper(testBase)
      jobID = chomper.createJob()
      graphfile = os.path.join(testdir, 'cGraph.graph')
      chomper.runGraph(jobID, graphfile)
      
      counter = 0
      while(chomper.isRunning([jobID])):
        time.sleep(0.1)
        counter += 1
        if counter>1000:
          self.assertTrue(False)
      
      state = chomper.doneSet[jobID]
      self.assertTrue('cA' in state['bin'])
      self.assertTrue('cB' in state['bin'])
      self.assertTrue('cC' in state['bin'])
    finally:
      shutil.rmtree(testBase)
      
      
  def test_chomperMany(self):
    testBase = os.path.join(testdir, str(uuid.uuid4()))
    try:
      chomper = jobChomper.chomper.Chomper(testBase, numWorkers=5)

      jobIDs = []
      for i in range(10):
        jobID = chomper.createJob()
        graphfile = os.path.join(testdir, 'cGraph.graph')
        chomper.runGraph(jobID, graphfile)
        jobIDs.append(jobID)

      counter = 0
      while(chomper.isRunning(jobIDs)):
        time.sleep(0.1)
        counter += 1
        if counter>1000:
          self.assertTrue(False)
      
      state = chomper.doneSet[jobID]
      self.assertTrue('cA' in state['bin'])
      self.assertTrue('cB' in state['bin'])
      self.assertTrue('cC' in state['bin'])
    finally:
      shutil.rmtree(testBase)