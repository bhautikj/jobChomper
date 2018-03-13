import os
testdir = os.path.dirname(__file__)
import unittest
import shutil
import uuid
import json
import random

import jobChomper.runGraph

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
  # def test_nostate(self):
  #   testBase, testTmp, testVar, testDone = createTestBase()
  #   try:
  #     directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
  #     jobID = directoryWrangler.createJob()
  #     runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
  #     self.assertRaises(ValueError, runGraph.loadState())
  #   finally:
  #     shutil.rmtree(testBase)
  #
  # def test_nograph(self):
  #   testBase, testTmp, testVar, testDone = createTestBase()
  #   try:
  #     directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
  #     jobID = directoryWrangler.createJob()
  #     jobDirectory = directoryWrangler.getVar(jobID)
  #     stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph"}
  #     stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
  #     with open(stateDictFile, 'w') as statedictwrite:
  #       statedictwrite.write(json.dumps(stateDict))
  #
  #     runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
  #     self.assertRaises(FileNotFoundError, runGraph.loadState())
  #   finally:
  #     shutil.rmtree(testBase)
      
  def test_graph(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph"}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, TRUE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_statedict_written(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.PENDINGKEY},
                      "A" : { "status" : jobChomper.node.PENDINGKEY},
                      "B" : { "status" : jobChomper.node.PENDINGKEY},
                      "C" : { "status" : jobChomper.node.PENDINGKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, TRUE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([jobChomper.graph.STARTNODENAME])
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_run(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.DONEKEY},
                      "A" : { "status" : jobChomper.node.PENDINGKEY},
                      "B" : { "status" : jobChomper.node.PENDINGKEY},
                      "C" : { "status" : jobChomper.node.PENDINGKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set(["A"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_run_A(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.DONEKEY},
                      "A" : { "status" : jobChomper.node.DONEKEY},
                      "B" : { "status" : jobChomper.node.PENDINGKEY},
                      "C" : { "status" : jobChomper.node.PENDINGKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set(["B","C"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)

  def test_graph_startnode_run_A_failed(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.DONEKEY},
                      "A" : { "status" : jobChomper.node.FAILEDKEY},
                      "B" : { "status" : jobChomper.node.PENDINGKEY},
                      "C" : { "status" : jobChomper.node.PENDINGKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set(["C"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_finished_with_failed(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.DONEKEY},
                      "A" : { "status" : jobChomper.node.FAILEDKEY},
                      "B" : { "status" : jobChomper.node.PENDINGKEY},
                      "C" : { "status" : jobChomper.node.DONEKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)

  def test_graph_startnode_finished_with_force_rerunFailed(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.DONEKEY},
                      "A" : { "status" : jobChomper.node.FAILEDKEY},
                      "B" : { "status" : jobChomper.node.PENDINGKEY},
                      "C" : { "status" : jobChomper.node.DONEKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(True)
      runInTheory = set(["A"])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)
      
  def test_graph_startnode_finished_(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.DONEKEY},
                      "A" : { "status" : jobChomper.node.DONEKEY},
                      "B" : { "status" : jobChomper.node.DONEKEY},
                      "C" : { "status" : jobChomper.node.DONEKEY}
                    }}
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      with open(stateDictFile, 'w') as filewrite:
        filewrite.write(json.dumps(stateDict))
      
      graphData  = "STARTNODE, A, FALSE\n"
      graphData += "A, B, TRUE\n"
      graphData += "A, C, FALSE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([])
      
      #print ("\n",toRun, runInTheory)
            
      self.assertTrue(toRun == runInTheory)
    finally:
      shutil.rmtree(testBase)

class TestRunGraphClasses(unittest.TestCase):      
  class rA(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("rA")
      return True

  class rB(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("rB")
      return True

  class rC(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("rC")
      return True
  
  class fA(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      if random.random() < 0.2:
        return False

      if "bin" not in params.keys():
        params["bin"] = []

      params["bin"].append("fA")
      return True

  class fB(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      if random.random() < 0.2:
        return False

      if "bin" not in params.keys():
        params["bin"] = []

      params["bin"].append("fB")
      return True

  class fC(jobChomper.node.Node):
    def __init__(self):
      super().__init__()
  
    def work(self, params):
      if random.random() < 0.2:
        return False

      if "bin" not in params.keys():
        params["bin"] = []
      params["bin"].append("fC")
      return True
      
  
  def test_graph_startnode_finished_(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = jobChomper.safeFileDict.SafeFileDict (
                  {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.PENDINGKEY},
                      "rA" : { "status" : jobChomper.node.PENDINGKEY},
                      "rB" : { "status" : jobChomper.node.PENDINGKEY},
                      "rC" : { "status" : jobChomper.node.PENDINGKEY}
                    }})
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      stateDict.enableJournal(stateDictFile)
      stateDict.writeJournal()
      
      graphData  = "STARTNODE, rA, FALSE\n"
      graphData += "rA, rB, TRUE\n"
      graphData += "rA, rC, TRUE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([jobChomper.graph.STARTNODENAME])
            
      self.assertTrue(toRun == runInTheory)
      
      runGraph.graphRun(False)
      
      self.assertTrue('rA' in runGraph.state['bin'])
      self.assertTrue('rB' in runGraph.state['bin'])
      self.assertTrue('rC' in runGraph.state['bin'])

    finally:
      shutil.rmtree(testBase)
      
      
  def test_graph_startnode_finished_randomFail(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      jobDirectory = directoryWrangler.getVar(jobID)
      stateDict = jobChomper.safeFileDict.SafeFileDict (
                  {jobChomper.runGraph.GRAPHFILEKEY : "test.graph",
                   jobChomper.node.JOBPROGRESSKEY: {
                      jobChomper.graph.STARTNODENAME : {"status" : jobChomper.node.PENDINGKEY},
                      "fA" : { "status" : jobChomper.node.PENDINGKEY},
                      "fB" : { "status" : jobChomper.node.PENDINGKEY},
                      "fC" : { "status" : jobChomper.node.PENDINGKEY}
                    }})
      stateDictFile = os.path.join(jobDirectory, jobChomper.runGraph.JOBSTATEFILE)
      stateDict.enableJournal(stateDictFile)
      stateDict.writeJournal()
      
      graphData  = "STARTNODE, fA, FALSE\n"
      graphData += "fA, fB, TRUE\n"
      graphData += "fA, fC, TRUE\n"
      
      graphFile = os.path.join(jobDirectory, "test.graph")
      with open(graphFile, 'w') as filewrite:
        filewrite.write(graphData)
        
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.loadState()
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([jobChomper.graph.STARTNODENAME])
            
      self.assertTrue(toRun == runInTheory)
      
      runGraph.graphRun(False)
      
      # print(runGraph.state)
      
      self.assertTrue('fA' in runGraph.state['bin'])
      self.assertTrue('fB' in runGraph.state['bin'])
      self.assertTrue('fC' in runGraph.state['bin'])

    finally:
      shutil.rmtree(testBase)
      
      
  def test_graph_startnode_finished_simplified(self):
    testBase, testTmp, testVar, testDone = createTestBase()
    try:
      directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobID = directoryWrangler.createJob()
      runGraph = jobChomper.runGraph.RunGraph(directoryWrangler, jobID)
      runGraph.initFromGraph(os.path.join(testdir, 'rGraph.graph'))
      runGraph.loadState()
      
      
      toRun = runGraph.graphWalk(False)
      runInTheory = set([jobChomper.graph.STARTNODENAME])
            
      self.assertTrue(toRun == runInTheory)
      
      runGraph.graphRun(False)
      
      self.assertTrue('rA' in runGraph.state['bin'])
      self.assertTrue('rB' in runGraph.state['bin'])
      self.assertTrue('rC' in runGraph.state['bin'])

    finally:
      shutil.rmtree(testBase)
      