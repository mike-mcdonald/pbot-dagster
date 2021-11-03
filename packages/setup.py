from setuptools import setup, find_packages

setup(
    name="dagster-packages",
    version="0.0.1",
    description="Dagster helper packages",
    author="Abdullah Malikyar",
    author_email="abdullah.malikyar@portlandoregon.gov",
    install_requires=["pycryptodome"],
    packages=find_packages(),
)
