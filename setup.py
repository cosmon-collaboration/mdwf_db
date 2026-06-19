# setup.py
from setuptools import setup, find_packages

setup(
    name='MDWFutils',
    version='1.0',
    packages=find_packages(),
    package_data={
        'MDWFutils': [
            'templates/**/*.j2',
            'build/grid_sources/*',
            'build/fixtures/*',
        ],
    },
    include_package_data=True,
    install_requires=[
        'pymongo>=4.6',
        'jinja2>=3.1',
        'pyyaml>=6.0',
        'pydantic>=2.0',
    ],
    entry_points={
        'console_scripts': [
            'mdwf_db = MDWFutils.cli:main',
        ],
    },
)