try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


setup(name='wpipe',
      version='1.0',
      packages=['wpipe', 'wpipe.sqlintf'],
      package_dir={'': 'src'},
      package_data={'wpipe': ['h5data/wpipe_store.h5']},
      install_requires=['numpy', 'pandas', 'tables', 'sqlalchemy', 'astropy'],
      scripts=['bin/create_pipeline']
      )
