from setuptools import setup

setup(
  name='checkpointer',
  version='0.1',
  packages=['checkpointer'],
  install_requires=[
    'numpy',
    'bcolz',
    'pymongo',
    'termcolor'
  ],
  zip_safe=False
)
