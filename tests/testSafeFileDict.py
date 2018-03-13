import os
testdir = os.path.dirname(__file__)
import unittest

import jobChomper.safeFileDict


class TestSafeFileDict(unittest.TestCase):  
      
  def test_dict_equality(self):
    x = {"a":"b"}
    y = jobChomper.safeFileDict.SafeFileDict()
    y = x
    self.assertTrue(y["a"] == x["a"] == "b")
    
    
  def test_dict_init_from_other(self):
    x = {"a":"b"}
    y = jobChomper.safeFileDict.SafeFileDict(x)
    self.assertTrue(y["a"] == x["a"] == "b")

  def test_weirdo_locks(self):
    y = jobChomper.safeFileDict.SafeFileDict({"a": "b"})
    y["c"] = y["a"]
    self.assertTrue(y["c"] == y["a"] == "b")
    
  
  def test_dict_write_read_journal(self):
    y = jobChomper.safeFileDict.SafeFileDict({"a": "b"})
    import uuid
    
    fn = os.path.join(testdir, str(uuid.uuid1()) + ".json")
    try:
      y.enableJournal(fn)
      y.writeJournal()
    
      z = jobChomper.safeFileDict.SafeFileDict({"c": "d"})
      z.readJournal(fn)
    finally:
      os.remove(fn)
    
    self.assertTrue(y == z)