# Development

## Build

- `rm Makefile.main; rm -rf .ci` to make sure you will have the last Makefile changes.
- `make dependencies` to download vendor dependencies using Glide.
- `make packages` to generate binaries for several platforms.

You will find the built binaries in `borges_linux_amd64/borges` and `borges_darwin_amd64/borges`.

If you're running borges for the first time, make sure you initialize the schema of the database first. You can do so by running the following command:

```
borges init
```

## Test

`make test`

Borges has 2 runtime dependencies and has tests that depend on them:

  - RabbitMQ

    Consumers and Producers interact through a Queue. You can run one in Docker with the following command:
    ```
    docker run -d --hostname rabbitmq --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3-management
    ```
    Note: a hostname needs to be provided, due to the fact that RabbitMQ stores data according to the host name


  - PostgreSQL

    Consumers creates [siva files](https://github.com/src-d/go-siva) with *Rooted Repositories*, but all repository metadata is stored in PostgreSQL.
    You can run one in Docker with the following command:
    ```
    docker run --name postgres  -e POSTGRES_DB=testing -e POSTGRES_USER=testing -e POSTGRES_PASSWORD=testing  -p 5432:5432 -d postgres
    # to check it manually, use
    docker exec -ti some-postgres psql -U testing
    ```

Use `make test-coverage` to run all tests and produce a coverage report.