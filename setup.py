from setuptools import setup

setup(
  name='checkpointer',
  packages=['checkpointer', 'checkpointer.storages'],
  version='0.3.1',
  author='Hampus Hallman',
  author_email='me@hampushallman.com',
  url='https://github.com/Reddan/checkpointer',
  license='MIT',
  install_requires=[
    'relib',
    'termcolor'
  ],
  python_requires='~=3.5',
)
