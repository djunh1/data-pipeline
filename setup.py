from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="data-pipeline",
    version="0.0.1",
    description="Market Data pipeline testing",
    extras_require=dict(tests=['pytest']),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://",
    author="Douglas Jacobson",
    author_email="douglas.jacobson2@protonmail.com",
    license="MIT",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "numpy>=1.21.2",
        "pandas>=1.3.3",
    ]
)
