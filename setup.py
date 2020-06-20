from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.19.0',
    'scipy==1.4.1',
    'numpy==1.18.1',
    'datetime==4.3.0'
]

setup(
    name='dataflow-ds2bq',
    version='1.0',
    description='My training application package.',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages()
)
