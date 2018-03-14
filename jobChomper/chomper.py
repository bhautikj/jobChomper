import logging
import logging.handlers
import os

import concurrent.futures

import jobChomper.directoryWrangler
import jobChomper.runGraph

LOGFILE = "chomper.log"
POOLSIZE = 5

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
  chomperPool = concurrent.futures.ThreadPoolExecutor(POOLSIZE)
  
  """ RunGraph Object """
  def __init__(self, workingDirectory, numWorkers = 5):
    chomperPool = concurrent.futures.ThreadPoolExecutor(numWorkers)
    self.workingDirectory = workingDirectory
    self.workTmp, self.workVar, self.workDone, self.logDir = createWorkDirs(self.workingDirectory)
    self.directoryWrangler = jobChomper.directoryWrangler.DirectoryWrangler(self.workVar, self.workTmp, self.workDone)
    
    logfile = os.path.join(self.logDir, LOGFILE)
    logging.basicConfig(format='%(asctime)s %(message)s', filename=logfile, level=logging.INFO)
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100*1024*1024, backupCount=10)
    logging.getLogger('').addHandler(handler)
    
    self.running = []
    
  def createJob(self):
    return self.directoryWrangler.createJob()
    
  def getJobPath(self, jobID):
    return self.directoryWrangler.getVar(jobID)

  def runGraph(self, jobID, graphFile, restartFailed = False):
    runGraph = jobChomper.runGraph.RunGraph(self.directoryWrangler)
    runGraph.initFromGraph(graphFile, jobID)     
    self.running.append(self.chomperPool.submit(runGraph.graphRun, restartFailed))     
    #runGraph.graphRun(restartFailed)
    #return runGraph.state
    
  def waitForComplete(self):
    done, not_done = concurrent.futures.wait(self.running)
    #print(done, not_done)
    
    results = []
    
    for job in done:
      results.append(job.result())
      
    return results
    