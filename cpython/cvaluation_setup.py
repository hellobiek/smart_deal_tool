from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
#export CFLAGS="-I /usr/local/lib/python3.7/site-packages/numpy/core/include $CFLAGS"
setup(
    cmdclass = {'build_ext': build_ext},
    #ext_modules = [Extension("cstock", ["cstock.pyx"], libraries=["m"], extra_compile_args=["-ffast-math"])]
    ext_modules = [Extension("cval", ["cval.pyx"], libraries=["m"], extra_compile_args=["-ffast-math", "-Wno-cpp", "-O3", "-march=native"])]
)
