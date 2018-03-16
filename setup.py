from distutils.core import setup

setup(
    # Application name:
    name="jobChomper",

    # Version number (initial):
    version="0.1.1",

    # Application author details:
    author="Bhautik Joshi",
    author_email="bjoshi@gmail.com",

    # Packages
    packages=["jobChomper"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://github.com/bhautikj/jobChomper",

    #
    license="LICENSE",
    description="Robustly queue & run a large number of jobs in parallel",

    # long_description=open("README.txt").read(),

    # Dependent packages (distributions)
    install_requires=[],
    
    python_requires='>=3.2',
)
