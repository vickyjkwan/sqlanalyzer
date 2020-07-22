import setuptools
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setuptools.setup(name='sqlanalyzer',
      version='0.5.0',
      description='A tool to parse and analyze the structure for Postgres sql queries.',
      classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Development Status :: 3 - Alpha',
      ],
      url='https://github.com/mathilda0902/sqlanalyzer',
      author='Vicky Kwan',
      author_email='vickyj.fan2016@gmail.com',
      license='MIT',
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=['sqlanalyzer'],
      install_requires=['sqlparse==0.3.0'],
      python_requires='>=3.5',
      zip_safe=False)
      