from setuptools import find_packages
from setuptools import setup

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='mediascope_api_lib',
    packages=find_packages(),
    version='1.3.1',
    description='Library for work with the Mediascope-Delivery-API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Sergey Poterianski',
    author_email='sergey.poterianski@mediascope.net',
    maintainer='Mediascope JSC',
    maintainer_email='github@mediascope.net',
    license='BSD-3-Clause',
    requires=['pandas', 'requests', 'pyparsing', 'urllib3'],
    install_requires=['pandas', 'requests', 'pyparsing', 'urllib3>=1.26.7'],
    url='https://github.com/MEDIASCOPE-JSC/mediascope-api-lib',
    download_url='https://github.com/MEDIASCOPE-JSC/mediascope-api-lib/tarball/v1.3.1',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3.6",
        "Natural Language :: Russian",
        "License :: OSI Approved :: BSD License",
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Operating System :: MacOS'
    ],
    python_requires='>=3.6'
)
