from setuptools import setup

setup(
    name='koerby',
    version='0.1',
    description='rdf based data integration framework',
    author='diggr',
    author_email='p.muehleder@ub.uni-leipzig.de',
    license='MIT',
    packages=['koerby'],
    entry_points="""
        [console_scripts]
        koerby=koerby.cli:cli
    """
)