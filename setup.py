from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.19.0'
]

setup(
    name='dataflow-ds2bq',
    version='1.0',
    description='Transfer the entities of Datastore to BigQuery.',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages()
)
