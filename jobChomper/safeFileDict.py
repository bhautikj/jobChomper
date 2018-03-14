import threading
import json
import logging

# see https://stackoverflow.com/a/2390997
class SafeFileDict(dict):  # dicts take a mapping or iterable as their optional first argument
  def __init__(self, *args, **kwargs):
    self.lock = threading.Lock()
    self.update(*args, **kwargs)
    self.doJournal = False
    self.journalFile = ""

  def enableJournal(self, journalFile):
     logging.info("Writing dict to: " + journalFile)
     self.journalFile = journalFile
     self.doJournal = True

  def writeJournal(self):
    if self.doJournal == False:
      logging.warning("Attempting to write Safe File Dict but no journal file specified.")
      return

    with self.lock:
      data = json.dumps (self, indent=2)
      with open(self.journalFile, 'w') as file:
        file.write(data)
        logging.debug("Wrote dict to: " + self.journalFile)
        

  def readJournal(self, journalFile=""):
    if journalFile == "" and self.doJournal == False:
      logging.warning("No journal file specified.")
      return
    
    if journalFile != "":
      toRead = journalFile
    else:
      toRead = self.journalFile
    
    with open(toRead, 'r') as file:
      self.clear()
      self.update(json.loads(file.read()))
      logging.info("Read dict from: " + toRead)
        
  def __getitem__(self, key):
    with self.lock:
      val = dict.__getitem__(self, key)
      # print ('GET', key)
      return val

  def __setitem__(self, key, val):
    with self.lock:
      # print ('SET', key, val)
      dict.__setitem__(self, key, val)

  def __repr__(self):
    with self.lock:
      dictrepr = dict.__repr__(self)
      return '%s(%s)' % (type(self).__name__, dictrepr)

  def update(self, *args, **kwargs):
    # print ('update', args, kwargs)
    for k, v in dict(*args, **kwargs).items():
      self[k] = v
