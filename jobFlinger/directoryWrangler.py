import os, uuid

import jobFlinger.runGraph

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
    
  def getVar(self, jobID):
    return os.path.join(self.varDir, jobID)

  def getTmp(self, jobID):
    return os.path.join(self.tmpDir, jobID)

  def getDone(self, jobID):
    return os.path.join(self.doneDir, jobID)
 
  def getActiveJobs(self):
    # just list directories in var
    subfolders = [f.path for f in os.scandir(self.varDir) if f.is_dir()]

    # only get the dirs with JOBSTATEFILE
    subfolders = [f for f in subfolders if os.path.exists(os.path.join(f, jobFlinger.runGraph.JOBSTATEFILE))]

    # remove dot files
    subfolders = [os.path.basename(f) for f in subfolders if f[0] != '.']
    
    return subfolders