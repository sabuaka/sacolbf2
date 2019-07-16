'''setup.py'''
from setuptools import setup
setup(
    name='sacolbf2',
    packages=['sacolbf2'],
    version='0.1.0',
    description='Collector library for bitFlyer',
    author='sabuaka',
    author_email='sabuaka-fx@hotmail.com',
    url="https://github.com/sabuaka/sacolbf2",
    install_requires=[
        'numpy==1.14.4',
        'pandas==0.23.4',
        'sautility@git+https://github.com/sabuaka/sautility.git',
        'saapibf@git+https://github.com/sabuaka/saapibf.git'
    ],
)
