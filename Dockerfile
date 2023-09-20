# Dagster libraries to run both dagit and the dagster-daemon. 
# Does not need to have access to any pipeline code.

FROM python:3.10-slim

# Set ${DAGSTER_HOME} and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/opt/dagster/ \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

COPY .certs/ /usr/local/share/ca-certificates
COPY requirements.txt ${DAGSTER_HOME}

RUN set -ex \
    && buildDeps='\
    build-essential \
    curl \
    gnupg \
    unixodbc-dev \
    ' \
    && runDeps='\
    unixodbc \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
    $buildDeps \
    $runDeps \
    && update-ca-certificates \
    # Install Microsoft ODBC driver
    && curl -fSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl -fSL https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && ACCEPT_EULA=Y apt-get install -yqq --no-install-recommends msodbcsql17 \
    && pip config set global.cert /etc/ssl/certs/ca-certificates.crt \
    && ls -al ${DAGSTER_HOME} \
    && pip install -r ${DAGSTER_HOME}/requirements.txt \
    && pip cache purge \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /var/tmp/* \
    /tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

COPY dagster.yaml workspace.yaml ${DAGSTER_HOME}

WORKDIR ${DAGSTER_HOME}
