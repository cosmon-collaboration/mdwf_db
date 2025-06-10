# setup.py
from setuptools import setup, find_packages

setup(
  name='MDWFutils',
  version='0.1',
  packages=find_packages(),
  entry_points={
    'console_scripts': [
      'mdwf_db = MDWFutils.cli:main',
    ],
  },
)