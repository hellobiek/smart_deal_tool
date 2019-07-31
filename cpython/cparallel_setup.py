import numpy as np
from distutils.core import setup
from Cython.Build import cythonize
from Cython.Distutils import build_ext
from distutils.extension import Extension
ext_modules=[Extension("cparallel", ["cparallel.pyx"], libraries = ["m"],
              extra_compile_args = ["-O3", "-ffast-math", "-Wno-cpp", "-march=native", "-fopenmp"],
              extra_link_args=['-fopenmp'], include_dirs=[np.get_include()])]

setup(
    name = "cparallel",
    cmdclass = {'build_ext': build_ext},
    ext_modules = ext_modules
)
