# framework [![Build Status](https://travis-ci.org/src-d/framework.svg?branch=master)](https://travis-ci.org/src-d/framework) [![codecov.io](https://codecov.io/gh/src-d/framework/branch/master/graph/badge.svg?token=am2H6bJkdp)](https://codecov.io/gh/src-d/framework)

**framework** provides abstractions to services used across multiple projects.

## Services

* **configurable** standarizes the way to create configuration containers.
* **database** package provides access to SQL databases.
* **queue** provides access to message brokers.

## Development

Run tests with:

    go test -v ./...

Tests require the following services running:

* PostgreSQL

```
docker run --name test-postgres -e POSTGRES_PASSWORD=testing -p 5432:5432 -e POSTGRES_USER=testing
```

* RabbitMQ

```
docker run --hostname rabbit --name rabbit -p 8080:15672 -p 5672:5672 rabbitmq:3-management
```

They also need the `etcd` binary present in `PATH`.

## License

Licensed under the terms of the Apache License Version 2.0. See the `LICENSE`
file for the full license text.
