from setuptools import setup, find_packages

def read_requirements():
    with open("requirements.txt") as req:
        content = req.read()
        requirements = content.split("\n")
    
    return requirements

setup(
    name='fdp',
    version='0.0.1',
    packages=find_packages(),
    install_requires=read_requirements(),
    entry_points={
        'console_scripts': [
            'fdp=fdp.cli:cli'
        ]
    }
)