#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module sets up the package for lib_database"""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(    
    name="lib_database",
    author="Justin Furuness, Matt Jaccino, Tony Zheng, Cameron Morris",
    author_email="jfuruness@gmail.com",
    version="0.0.1",
    url="https://github.com/jfuruness/lib_database.git",
    download_url='https://github.com/jfuruness/lib_database.git',
    description="Wrapper around a postgres database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'numpy',
        'psutil',
        'psycopg2-binary',
        'pytest',
    ],
    classifiers=[
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3'],
    entry_points={
        'console_scripts': [
        ]},
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=['pytest'],
)
