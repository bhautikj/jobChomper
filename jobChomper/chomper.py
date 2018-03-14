import logging
import logging.handlers
import os

import jobChomper.directoryWrangler
import jobChomper.runGraph

LOGFILE = "chomper.log"

def createWorkDirs(workingDirectory):
  testTmp = os.path.join(workingDirectory, "tmp")
  os.makedirs(testTmp, exist_ok=True)
  testVar = os.path.join(workingDirectory, "var")
  os.makedirs(testVar, exist_ok=True)
  testDone = os.path.join(workingDirectory, "done")
  os.makedirs(testDone, exist_ok=True)
  testLog = os.path.join(workingDirectory, "log")
  os.makedirs(testLog, exist_ok=True)
  
  return testTmp, testVar, testDone, testLog
  
class Chomper(object):  
  """ RunGraph Object """
  def __init__(self, workingDirectory, numWorkers = 5):
    self.workingDirectory = workingDirectory
    self.workTmp, self.workVar, self.workDone, self.logDir = createWorkDirs(self.workingDirectory)
    self.directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(self.workVar, self.workTmp, self.workDone)
    
    logfile = os.path.join(self.logDir, LOGFILE)
    logging.basicConfig(format='%(asctime)s %(message)s', filename=logfile, level=logging.INFO)
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100*1024*1024, backupCount=10)
    logging.getLogger('').addHandler(handler)
    
  def createJob(self):
    return self.directoryWrangler.createJob()
    
  def getJobPath(self, jobID):
    return self.directoryWrangler.getVar(jobID)
    
  def runGraph(self, jobID, graphFile, restartFailed = False):
    runGraph = jobChomper.runGraph.RunGraph(self.directoryWrangler)
    runGraph.initFromGraph(graphFile, jobID)          
    runGraph.graphRun(restartFailed)
    return runGraph.state