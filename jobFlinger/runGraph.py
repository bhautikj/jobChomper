import jobFlinger.graph
import jobFlinger.node
import jobFlinger.safeFileDict

import os, uuid

JOBSTATEFILE = "state.json"

#
# For jobFlinger we need:
#  A) a var dir - jobs in progress live here and can survive application failure
#  B) a tmp dir - somewhere to store tmp data; ephemeral data that can be lost
#  C) a done dir - somewhere to move jobs after they are done
#
# * Each job gets a UUID
# * Within var and tmp we generate a directory with the UUID of the job
#
class DirectoryWrangler(object):
  """ DirectoryWrangler Object """
  def __init__(self, varDir, tmpDir, doneDir):
    # store as absolute paths - these will always work if passed
    # to external calls, whereas relative paths are a crapshoot
    self.varDir = os.path.abspath(varDir)
    self.tmpDir = os.path.abspath(tmpDir)
    self.doneDir = os.path.abspath(doneDir)
  
  def createJob(self):
    jobID = str(uuid.uuid4())
    jobDir = os.path.join(self.varDir, jobID)
    os.mkdir(jobDir)
    return jobID
    
  def getJobVar(self, jobID):
    return os.path.join(self.varDir, jobID)

  def getJobTmp(self, jobID):
    return os.path.join(self.tmpDir, jobID)

  def getJobDone(self, jobID):
    return os.path.join(self.doneDir, jobID)
 
  def getActiveJobs(self):
    # just list directories in var
    subfolders = [f.path for f in os.scandir(self.varDir) if f.is_dir()]

    # only get the dirs with JOBSTATEFILE
    subfolders = [f for f in subfolders if os.path.exists(os.path.join(f, JOBSTATEFILE))]

    # remove dot files
    subfolders = [os.path.basename(f) for f in subfolders if f[0] != '.']
    
    return subfolders
    
   
#
# when we run a graph, we create a working directory in var OR it gets passsed in (in the case of failed jobs)
# return that directory so data can be put into it before execution
#
class RunGraph(object):
  """ RunGraph Object """
  def __init__(self, graphObject, workingDir):
    self.graph = graphObject
    # build state object
    self.state = jobFlinger.safeFileDict.SafeFileDict()
    stateFile = os.path.join(workingDir, 'state.json')