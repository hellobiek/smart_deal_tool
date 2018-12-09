import numpy as np
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
setup(
    cmdclass = {'build_ext': build_ext},
    #ext_modules = [Extension("cchip", ["cchip.pyx"], libraries=["m"], extra_compile_args=["-ffast-math"])]
    ext_modules = [Extension("cchip", ["cchip.pyx"], libraries=["m"], extra_compile_args=["-ffast-math", "-Wno-cpp"], include_dirs=[np.get_include()])]
)
