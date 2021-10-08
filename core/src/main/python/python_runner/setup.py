import os
import setuptools

setuptools.setup(
    name="akdl",
    version="0.0.1",
    description="Alink Deep-Learning Support API",
    packages=setuptools.find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6,<3.8',
    install_requires=[
    ],
    include_package_data=True
)
