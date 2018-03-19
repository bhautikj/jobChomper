import os, shutil

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

    # set of running futures
    # maps futures to job ID's
    self.runningSet = {}
    
    # when completed, moved to done set
    self.doneSet = {}

  def configureLogger(self):
    import logging
    import logging.handlers
    logfile = os.path.join(self.logDir, LOGFILE)
    logformat = '%(asctime)s %(message)s'
    logging.basicConfig(format=logformat, filename=logfile, level=logging.INFO)
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100*1024*1024, backupCount=10)
    handler.setFormatter(logging.Formatter(logformat))
    logging.getLogger('').addHandler(handler)
    
  def createJob(self):
    return self.directoryWrangler.createJob()
    
  def getJobPath(self, jobID):
    return self.directoryWrangler.getVar(jobID)

  def jobDone(self, future):
    if future not in self.runningSet.keys():
      raise ValueError("Recived job complete signal for job not in Chomper")
    
    jobID = self.runningSet[future]
    self.doneSet[jobID] = future.result()
    
    # move job to done
    self.doneSet[jobID].disableJournal()
    jobdir = self.directoryWrangler.getVar(jobID)
    workdir = self.directoryWrangler.getDone(jobID)
    shutil.move(jobdir, workdir)

  def runGraph(self, jobID, graphFile, restartFailed = False):
    runGraph = jobChomper.runGraph.RunGraph(self.directoryWrangler)
    runGraph.initFromGraph(graphFile, jobID)
    future = self.chomperPool.submit(runGraph.graphRun, restartFailed)
    future.add_done_callback(self.jobDone)
    self.runningSet[future] = jobID

  def isRunning(self, jobIDs = None):
    if jobIDs == None:
      jobIDs = list(self.runningSet.values())
    for future in self.runningSet.keys():
      jid = self.runningSet[future]
      if jid in jobIDs:
        if future.done() is False:
          return True
    return False

  def waitForAllComplete(self):    
    done, not_done = concurrent.futures.wait(self.runningSet.keys())
    
    results = []
    
    for job in done:
      results.append(job.result())
      
    return results
    