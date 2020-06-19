import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(name='sqlanalyzer',
      version='0.25',
      description='A tool to parse and analyze the structure for Postgres sql queries.',
      classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 3 - Alpha',
      ],
      url='https://github.com/mathilda0902/sqlanalyzer',
      author='Vicky Kwan',
      author_email='vickyj.fan2016@gmail.com',
      license='MIT',
      packages=['sqlanalyzer'],
      install_requires=['sqlparse==0.3.1'],
      python_requires='>=3.5',
      zip_safe=False)
      