import setuptools

with open("README.md", "r", encoding='utf8') as fh:
    long_description = fh.read()

setuptools.setup(
    name="DWX_ZeroMQ_Connector_kmotr",  # Replace with your own username
    version="2.1.0",
    author="kmotr",
    author_email="author@example.com",
    description="Zero MQ Connector to MetaTrader4. Fork from https://github.com/darwinex/dwx-zeromq-connector.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Kmotr/dwx-zeromq-connector",
    packages=setuptools.find_packages(),
    package_data={
        "": ["logging.json"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: BSD 3-Clause",
        "Operating System :: Windows 7",
    ],
    python_requires='>=3.6',
)
