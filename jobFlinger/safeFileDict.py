#
# inspired by/cribbed from
# https://stackoverflow.com/questions/3387691/how-to-perfectly-override-a-dict
#

from itertools import chain
try:              # Python 2
    str_base = basestring
    items = 'iteritems'
except NameError: # Python 3
    str_base = str, bytes, bytearray
    items = 'items'

import threading

_RaiseKeyError = object() # singleton for no-default behavior

class SafeFileDict(dict):  # dicts take a mapping or iterable as their optional first argument
  __slots__ = () # no __dict__ - that would be redundant
  lock = threading.Lock()
  @staticmethod # because this doesn't make sense as a global function.
  def _process_args(mapping=(), **kwargs):
    if hasattr(mapping, items):
      mapping = getattr(mapping, items)()
    return ((k, v) for k, v in chain(mapping, getattr(kwargs, items)()))
  def __init__(self, mapping=(), **kwargs):
    super(SafeFileDict, self).__init__(self._process_args(mapping, **kwargs))
  def __getitem__(self, k):
    with self.lock:
      item = super(SafeFileDict, self).__getitem__(k)
      return item
  def __setitem__(self, k, v):
    with self.lock:
      item = super(SafeFileDict, self).__setitem__(k, v)
      return item
  def __delitem__(self, k):
    return super(SafeFileDict, self).__delitem__(k)
  def get(self, k, default=None):
    return super(SafeFileDict, self).get(k, default)
  def setdefault(self, k, default=None):
    return super(SafeFileDict, self).setdefault(k, default)
  def pop(self, k, v=_RaiseKeyError):
    if v is _RaiseKeyError:
      return super(SafeFileDict, self).pop(k)
    return super(SafeFileDict, self).pop(k, v)
  def update(self, mapping=(), **kwargs):
    super(SafeFileDict, self).update(self._process_args(mapping, **kwargs))
  def __contains__(self, k):
    return super(SafeFileDict, self).__contains__(k)
  def copy(self): # don't delegate w/ super - dict.copy() -> dict :(
    return type(self)(self)
  @classmethod
  def fromkeys(cls, keys, v=None):
    return super(SafeFileDict, cls).fromkeys((k for k in keys), v)
  def __repr__(self):
    return '{0}({1})'.format(type(self).__name__, super(SafeFileDict, self).__repr__())
