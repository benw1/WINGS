#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

# __version__ is set in the below file, but use this here to avoid warnings.
__version__ = None
exec(open("src/wpipe/__version__.py").read())

setup(name='wpipe',
      version=__version__,
      packages=['wpipe', 'wpipe.sqlintf'],
      package_dir={'': 'src'},
      install_requires=['numpy', 'pandas', 'tables', 'sqlalchemy',
                        'mysql-connector-python', 'mysqlclient', 'pymysql', 'astropy'],
      scripts=['bin/wingspipe']
      )
