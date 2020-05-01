try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


setup(name='wpipe',
      version='1.0',
      packages=['wpipe', 'wpipe.sqlintf'],
      package_dir={'': 'src'},
      install_requires=['numpy', 'pandas', 'tables', 'sqlalchemy', 'astropy'],
      scripts=['bin/wingspipe']
      )
