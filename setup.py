from setuptools import setup

setup(name='sqlanalyzer',
      version='0.1',
      description='A tool to parse and analyze the structure for Postgres sql queries.',
      classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Topic :: SQL Query Processing :: Data Lineage Management',
      ],
      url='https://github.com/mathilda0902/sqlanalyzer',
      author='Vicky Kwan',
      author_email='vickyj.fan2016@gmail.com',
      license='MIT',
      packages=['sqlanalyzer'],
      install_requires=['sqlparse==0.3.0'],
      zip_safe=False)
      