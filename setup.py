import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="deadsimpledb",
    version="0.0.1",
    author="Brandyn Kusenda",
    author_email="bkusenda@gmail.com",
    description="A key value database like interface for storing data to the file system.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bkusenda/deadsimpledb",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    license='MIT',
    install_requires=['simplejson>=3.17.0',],
    python_requires='>=3.7',
)