from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    cmdclass = {'build_ext': build_ext},
    ext_modules = [Extension(
        "roqet",
        ["roqet.pyx"],
        libraries=["raptor", "rasqal"],
        library_dirs 	= ['/usr/local/Cellar/rasqal/0.9.15/lib', '/usr/local/Cellar/raptor/1.4.21/lib/'],

        include_dirs        = ['/usr/local/Cellar/rasqal/0.9.15/include/rasqal/', '/usr/local/Cellar/raptor/1.4.21/include/']
        )
    ]
)
