from setuptools import setup
from os.path import dirname, abspath, join

base_path = dirname(abspath(__file__))

with open(join(base_path, "README.md")) as readme_file:
    readme = readme_file.read()

with open(join(base_path, "requirements.txt")) as req_file:
    requirements = req_file.readlines()

setup(
    name='koerby',
    version='0.1',
    description='rdf based data integration framework',
    long_description=readme,
    author='diggr',
    author_email='p.muehleder@ub.uni-leipzig.de',
    install_requires=requirements,
    license='MIT',
    packages=['koerby'],
    entry_points="""
        [console_scripts]
        koerby=koerby.cli:cli
    """
)
