import os
testdir = os.path.dirname(__file__)
import unittest
import shutil
import uuid
import json

import jobFlinger.runGraph

def createTestBase():
  testBase = os.path.join(testdir, str(uuid.uuid4()))
  os.mkdir(testBase)
  
  testTmp = os.path.join(testBase, "tmp")
  os.mkdir(testTmp)
  testVar = os.path.join(testBase, "var")
  os.mkdir(testVar)
  testDone = os.path.join(testBase, "done")
  os.mkdir(testDone)
  
  return testBase, testTmp, testVar, testDone

class TestRunGraph(unittest.TestCase):
  def test_nostate(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      self.assertRaises(ValueError, jobFlinger.runGraph.RunGraph, directoryWrangler, jobID)
    finally:
      shutil.rmtree(testBase)

  def test_nograph(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph" }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as statedictwrite:
        statedictwrite.write(json.dumps(stateDict))
      
      self.assertRaises(FileNotFoundError, jobFlinger.runGraph.RunGraph, directoryWrangler, jobID)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph" }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, TRUE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
    finally:
      shutil.rmtree(testBase)