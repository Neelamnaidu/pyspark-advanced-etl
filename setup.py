# setup.py

from setuptools import setup, find_packages

setup(
    name='your_package_name',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        # add other dependencies here
    ],
    entry_points={
        'console_scripts': [
            'your_script=your_module:main',
        ],
    },
)