#!/usr/bin/env python
import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# metadata are set in the below file, but use this here to avoid warnings.
__author__ = __copyright__ = __credits__ = __license__ = __version__ = __maintainer__ = __email__ = __status__ = None
exec(open(os.path.split(os.path.abspath(__file__))[0] + "/src/wpipe/__metadata__.py").read())

long_description = ""

setup(name='wpipe',
      version=__version__,
      author=__author__,
      author_email=__email__,
      maintainer=__maintainer__,
      maintainer_email=__email__,
      url="https://github.com/benw1/WINGS",
      description="Provides a suite of classes to deploy the WINGS pipeline functionalities.",
      long_description=long_description,
      long_description_content_type="text/markdown",
      classifiers=[
          __status__,
          "Environment :: Console",
          "Intended Audience :: Science/Research",
          __license__,
          "Natural Language :: English",
          "Operating System :: Unix",
          "Programming Language :: Python :: 3",
          "Topic :: Database",
          "Topic :: Scientific/Engineering :: Astronomy",
          "Topic :: Software Development :: Version Control :: Git"
      ],
      python_requires='>=3',
      packages=['wpipe', 'wpipe.sqlintf', 'wpipe.proxies', 'wpipe.scheduler'],
      package_dir={'': 'src'},
      install_requires=['numpy', 'pandas', 'tenacity', 'tables', 'sqlalchemy', 'pymysql',
                        'mysql-connector-python', 'mysqlclient', 'astropy', 'jinja2'],
      scripts=['bin/wingspipe', 'bin/pbsconsumer.py', 'bin/slurmconsumer.py']
      )
