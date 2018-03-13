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
      
      toRun = runGraph.graphWalk(False)
      
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_statedict_written(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.PENDINGKEY},
                      "A" : { "status" : jobFlinger.node.PENDINGKEY},
                      "B" : { "status" : jobFlinger.node.PENDINGKEY},
                      "C" : { "status" : jobFlinger.node.PENDINGKEY}
                    } }
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
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([jobFlinger.graph.STARTNODENAME])
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_run(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.DONEKEY},
                      "A" : { "status" : jobFlinger.node.PENDINGKEY},
                      "B" : { "status" : jobFlinger.node.PENDINGKEY},
                      "C" : { "status" : jobFlinger.node.PENDINGKEY}
                    } }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set(["A"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_run_A(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.DONEKEY},
                      "A" : { "status" : jobFlinger.node.DONEKEY},
                      "B" : { "status" : jobFlinger.node.PENDINGKEY},
                      "C" : { "status" : jobFlinger.node.PENDINGKEY}
                    } }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set(["B","C"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)

  def test_graph_startnode_run_A_failed(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.DONEKEY},
                      "A" : { "status" : jobFlinger.node.FAILEDKEY},
                      "B" : { "status" : jobFlinger.node.PENDINGKEY},
                      "C" : { "status" : jobFlinger.node.PENDINGKEY}
                    } }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set(["C"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_finished_with_failed(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.DONEKEY},
                      "A" : { "status" : jobFlinger.node.FAILEDKEY},
                      "B" : { "status" : jobFlinger.node.PENDINGKEY},
                      "C" : { "status" : jobFlinger.node.DONEKEY}
                    } }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)

  def test_graph_startnode_finished_with_force_rerunFailed(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.DONEKEY},
                      "A" : { "status" : jobFlinger.node.FAILEDKEY},
                      "B" : { "status" : jobFlinger.node.PENDINGKEY},
                      "C" : { "status" : jobFlinger.node.DONEKEY}
                    } }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
      
      toRun = runGraph.graphWalk(True)
      runInTheory = set(["A"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_finished_(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobFlinger.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = { jobFlinger.runGraph.GRAPHFILEKEY : "test.graph",
                    jobFlinger.node.JOBPROGRESSKEY: {
                      jobFlinger.graph.STARTNODENAME : { "status" : jobFlinger.node.DONEKEY},
                      "A" : { "status" : jobFlinger.node.DONEKEY},
                      "B" : { "status" : jobFlinger.node.DONEKEY},
                      "C" : { "status" : jobFlinger.node.DONEKEY}
                    } }
      stateDictFile = os.path.join(jobDirectory, jobFlinger.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobFlinger.runGraph.RunGraph(directoryWrangler, jobID)
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)