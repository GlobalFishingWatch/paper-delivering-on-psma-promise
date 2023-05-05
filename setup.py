# Stub so the pip install -e works ; real action is in setup.cfg
import os

from setuptools import setup

os.chdir(os.path.dirname(os.path.abspath(__file__)))

setup()
