import binascii as ba

def pp_tuple(triple):
    return tuple(i for i in t2hex(triple))

def pp_id2s(triple,stringstore):
    return tuple(stringstore.id2s(i) for i in triple)
        
def t2hex(triple):
    for key in triple:
        if key is None:
            yield "None"
        else:
            yield ba.hexlify(key)


class memoized(object):
   """Decorator that caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned, and
   not re-evaluated.
   """
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      try:
         return self.cache[args]
      except KeyError:
         value = self.func(*args)
         self.cache[args] = value
         return value
      except TypeError:
         # uncachable -- for instance, passing a list as an argument.
         # Better to not cache than to blow up entirely.
         return self.func(*args)
   def __repr__(self):
      """Return the function's docstring."""
      return self.func.__doc__
   def __get__(self, obj, objtype):
      """Support instance methods."""
      return functools.partial(self.__call__, obj)   