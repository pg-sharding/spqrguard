ARG PG_VERSION=14
FROM postgres:${PG_VERSION}

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

COPY . /spqrguard
WORKDIR /spqrguard

RUN make USE_PGXS=1 && make USE_PGXS=1 install

ENTRYPOINT [ "/spqrguard/docker/regress/run_tests.sh" ]
