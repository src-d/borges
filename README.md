
borges [![Build Status](https://travis-ci.org/src-d/borges.svg?branch=master)](https://travis-ci.org/src-d/borges) [![codecov.io](https://codecov.io/gh/src-d/borges/branch/master/graph/badge.svg?token=ETL49e3u1L)](https://codecov.io/gh/src-d/borges)
======

**borges** archives repositories in a universal git library.

> I have always imagined that Paradise will be a kind of library.

borges uses the producer/consumer pattern, where a producer generates jobs and
one or more consumers get the jobs and executes them. Each borges job triggers
the update of a repository.

Read the borges package godoc for further details on how borges archives
repositories.

## CLI

Run `borges --help` to get help about the main commands (producer and consumer)
and their options.

## Producer

The producer runs as a service. It determines which repositories should be
updated next and enqueues new jobs for them.

## Consumer

The consumer runs as a service. It gets jobs from the queue and dispatches them
to a worker pool.

Each job is a request to update a repository. It can be a new or an existing
one. The repository is fetched (incrementally when possible) and each reference
is pushed to a local repository dedicated to all references from all repositories
that share the same **init commit**.

Note that borges should be the only one creating and writing to our repository
storage.

## Administration Notes

Both the producer and consumer services will run even if they cannot connect to
the queue, or even if the queue is malfunctioning. If the queue does not work,
they will just retry until it does.

# Development

## Build

Build:

`make packages`

You will find binaries in `borges_linux_amd64/borges` and `borges_darwin_amd64/borges`.

If running for the first time, you also need to add table to PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS repositories (
    id uuid PRIMARY KEY,
    created_at timestamptz,
    updated_at timestamptz,
    endpoints text[],
    status varchar(20),
    fetched_at timestamptz,
    fetch_error_at timestamptz,
    last_commit_at timestamptz,
    _references jsonb
    );
```

## Test

`make test`

Borges has 2 runtime dependencies and have tests depending on them:
  - RabbitMQ

    Consumers and Producers interact though a Queue. You can run one in Docker by
    ```
    docker run -d --hostname rabbit --name rabbit -p 8080:15672 -p 5672:5672 rabbitmq:3-management
    ```
    Note: a hostname is provided, due to fact that rabbitmq stores data according to the host name


  - PostgreSQL

    Consumers make SIVA files with RootedRepositories, but all repository metadata is stored in PostgreSQL
    You can run one in Docker by
    ```
    docker run --name postgres -e POSTGRES_PASSWORD=testing -p 5432:5432 -e POSTGRES_USER=testing -d postgres
    # to check it manually, use
    docker exec -ti some-postgres psql -U testing
    ```

`make test-coverage` to produce a coverage report
