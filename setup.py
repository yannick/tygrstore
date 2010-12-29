from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

ext_modules = [Extension("stringstore", ["stringstore.pyx"]),
               Extension("query_engine", ["query_engine.pyx"]),
               Extension("index", ["index.pyx"]),
                  Extension("index_manager", ["index_manager.pyx"])]

setup(
  name = 'Hello world app',
  cmdclass = {'build_ext': build_ext},
  ext_modules = ext_modules
)