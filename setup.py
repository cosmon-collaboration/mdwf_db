# setup.py
from setuptools import setup, find_packages

setup(
    name='MDWFutils',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'pymongo>=4.6',
        'jinja2>=3.1',
        'pyyaml>=6.0',
    ],
    entry_points={
        'console_scripts': [
            'mdwf_db = MDWFutils.cli:main',
        ],
    },
)