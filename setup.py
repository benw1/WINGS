try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='wpipe',
      version='1.0',
      packages=['wpipe'],
      package_dir={'wpipe': 'src/wpipe'},
      package_data={'wpipe': ['h5data/wpipe_store.h5']},
      install_requires=['numpy','pandas'],
      )


