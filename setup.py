from setuptools import setup

setup(name='sqlanalyzer',
      version='0.1',
      description='A tool to parse and analyze the structure for Postgres sql queries.',
      url='https://github.com/mathilda0902/sqlanalyzer',
      author='mathilda0902',
      author_email='vickyj.fan2016@gmail.com',
      license='MIT',
      packages=['sqlanalyzer'],
      install_requires=['sqlparse==0.3.0'],
      zip_safe=False)