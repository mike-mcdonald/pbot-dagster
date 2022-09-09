# PBOT Dagster Pipelines

This repository contains all code needed to run PBOT's internal Dagster pipelines.

## Getting started

### Requirements

- Conda or Mamba

### Setup

If this is the first time opening this repository in this location run the following from the root directory to create a new virtual environment in the `.venv` folder:

```
conda env create --prefix .\.venv --file environment.yml
conda activate --prefix .\.venv
```

If you have a `.venv` folder already present, run the following form the root directory to update dependencies:

```
conda activate --prefix .\.venv
conda env update --prefix .\.venv --file environment.yml  --prune
```

Finally run the following to install all the necessary packages for development:

```
pip install -r .\requirements.txt
pip install -r .\requirements-dev.txt
```
