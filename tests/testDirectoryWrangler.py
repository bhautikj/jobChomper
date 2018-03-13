import os
testdir = os.path.dirname(__file__)
import unittest
import shutil
import uuid

import jobChomper.directoryWrangler
import jobChomper.runGraph

def touch(fname):
    open(fname, 'a').close()
    os.utime(fname, None)

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

class TestDirectoryWrangler(unittest.TestCase):
  def test_init(self):
    testBase, testTmp, testVar, testDone = createTestBase()
        
    try:
      w = jobChomper.directoryWrangler.DirectoryWrangler(testVar, testTmp, testDone)
      jobA = w.createJob()
      self.assertTrue(os.path.isdir(os.path.join(testVar, jobA)))
      touch(os.path.join(testVar, jobA, jobChomper.runGraph.JOBSTATEFILE))
      jobB = w.createJob()
      self.assertTrue(os.path.isdir(os.path.join(testVar, jobB)))
      touch(os.path.join(testVar, jobB, jobChomper.runGraph.JOBSTATEFILE))
      
      jobs = set(w.getActiveJobs())
      ejobs = set([jobA, jobB])
      self.assertTrue(jobs == ejobs)      
    finally:
      shutil.rmtree(testBase)