
borges [![Build Status](https://travis-ci.org/src-d/borges.svg?branch=master)](https://travis-ci.org/src-d/borges) [![codecov.io](https://codecov.io/gh/src-d/borges/branch/master/graph/badge.svg?token=ETL49e3u1L)](https://codecov.io/gh/src-d/borges) [![GitHub version](https://badge.fury.io/gh/src-d%2Fborges.svg)](https://github.com/src-d/borges/releases)
======

**borges** collects and stores Git repositories.

> I have always imagined that Paradise will be a kind of library.

Borges is a set of tools for collection and storage of Git repositories at large scale.
It is a distributed system, similar to a search engine, that uses a custom
repository storage [file format](https://blog.sourced.tech/post/siva/) and is optimized
for saving storage space and keeping repositories up-to-date.

## Key concepts

 - **Borges producer**: a standalone process that reads repository URLs (from RabbitMQ or file) and schedules fetching this repository.

 - **Borges consumer**: a standalone process that takes URLs from RabbitMQ, clones remote repository and pushes it to the appropriate *Rooted Repository* in the storage (local filesystem or HDFS).

 - **Borges packer**: a standalone process that takes repository paths (or URLs) from a file and packs them into siva files (as a *Rooted Repository*) in the given output directory.

 - **Rooted Repository**: a standard Git repository that stores all objects from all repositories that share common history, identified by same initial commit. It is stored in [Siva](https://github.com/src-d/go-siva) file format.

   ![Root Repository explanatory diagram](https://user-images.githubusercontent.com/5582506/30617179-2aba194a-9d95-11e7-8fd5-0a87c2a595f9.png)

Consumer and Producer run independently, communicating though a RabbitMQ instance
and storing repository meta-data in PostgreSQL.

Packer does not need a RabbitMQ or a PostgreSQL instance and is not meant to be used as a pipeline, that's what consumer and producer are meant for.

Read the borges package godoc for further details on how does borges archive
the repositories.

## CLI

Both producer and consumer are shipped as a single binary,
see `borges --help` to get details about the main commands and their options.

## Setting up borges

Borges needs a database and a message broker to do its job.
It works with a PostgreSQL database by default and uses RabbitMQ.
You can use the following environment variables to configure those:
* `CONFIG_DBUSER`, by default: `testing`
* `CONFIG_DBPASS`, by default: `testing`
* `CONFIG_DBHOST`, by default: `0.0.0.0`
* `CONFIG_DBPORT`, by default: `5432`
* `CONFIG_DBNAME`, by default: `testing`
* `CONFIG_DBSSLMODE`, by default: `disable`
* `CONFIG_DBAPPNAME`, by default: ``
* `CONFIG_DBTIMEOUT`, by default: `30s`

Other important settings are:
* `CONFIG_TEMP_DIR`: Local path to store temporal files needed by the Borges consumer, by default: `/tmp/sourced`
* `CONFIG_CLEAN_TEMP_DIR`: Delete temporay directory before starting, by default: `false`
* `CONFIG_BROKER`: by default: `amqp://localhost:5672`
* `CONFIG_ROOT_REPOSITORIES_DIR`: .siva file storage. If no HDFS connection url is provided, this will be a path in local filesystem. Otherwise, it will be an HDFS directory, by default: `/tmp/root-repositories`
* `CONFIG_ROOT_REPOSITORIES_TEMP_DIR`: where `siva.copy` files are temporary placed. Only needed for HDFS.
* `CONFIG_LOCKING`, by default: `local:`, other options: `etcd:`
* `CONFIG_HDFS`: (host:port) If this property is not provided, all root repositories will be stored into the local filesystem, by default: `""`
* `CONFIG_BUCKETSIZE`, by default: `0`, number of characters used from the siva file name to create bucket directories. The value `0` means that all files will be saved at the same level.
* `LOG_LEVEL`: Minimum log level that is printed, by default: `info`.
* `LOG_FORMAT`: Format to print logs (`text` or `json`), by default: `text`.

**Note:** This version is only compatible with rovers >= 2.6.2. It will also have problems with RabbitMQ queues created by previous versions.

## Producer

The producer runs as a service. It determines which repositories should be
updated next and enqueues new jobs for them.

To launch the producer you just have to run it with the default configuration:

    borges producer mentions

Producer reads [mentions](https://github.com/src-d/core-retrieval/blob/master/model/mention.go) from [rovers](https://github.com/src-d/rovers)'s RabbitMQ queue, but it can also read URLs directly from a file with the special CLI option:

    borges producer file /path/to/file

The file must contain a url per line, it looks like:

```
https://github.com/a/repo1
https://github.com/b/repo2.git
http://github.com/c/repo3
http://github.com/d/repo4.git
```

You can change the priority of jobs produced with `--priority` option. It is a number from 0 to 8 where 0 is the lowest priority:

    borges producer file --priority 8 /path/to/file

When jobs fail they're sent to the buried queue. If you want to requeue them, you can pass the `--republish-buried` flag (this only works for the `mentions` source). For example:

```
borges producer --republish-buried
```

So a possible command to launch the producer could be:

```bash
$ CONFIG_DBUSER="user" \
CONFIG_DBPASS="pass" \
CONFIG_DBHOST="postgres" \
CONFIG_DBNAME="borges-db"  \
CONFIG_BROKER="amqp://guest:guest@rabbitmq:5672" \
LOG_LEVEL=debug \
borges producer mentions
```

For more details, use `borges producer -h`.

## Consumer

The consumer runs as a service. It gets jobs from the queue and dispatches them
to a goroutine workers pool.

Each job is a request to update a repository. It can be a new or an existing
one. The remote repository is fetched (incrementally when possible) and each reference
is then pushed to a specific [Rooted Repository](#key-concepts), dedicated to storing all references from repositories that share the same *initial commit*.

Note that borges should be the only one creating and writing to the repository
storage.

To launch a consumer instance from the command line with default configuration:

    borges consumer

You can select the number of workers to use, by default it uses 8:

    borges consumer --workers=20

A command you could use to run it could be:

```bash
$ CONFIG_TEMP_DIR="/borges/tmp"  \
CONFIG_ROOT_REPOSITORIES_DIR="/borges/root-repositories"  \
LOG_LEVEL=debug \
borges consumer --workers=20
```

For more details, use `borges consumer -h`

## Packer

The packer runs as a one time command getting jobs from a file with a repository path (or URL) per line and distributes these jobs across many workers to group them into *Rooted Repositories* and pack them as siva files.

This command does not need a PostgreSQL or a RabbitMQ connection and can be used locally without internet connection if all the repositories to pack are local.

Imagine we have the following file `repos.txt` with the repositories we want to pack:

```
git://github.com/yada/yada.git
https://github.com/foo/bar
file:///home/me/some-repo
/home/me/another-repo
```
If no protocol is specified it will be treated as an absolute path to a repository, which can be a bare repository or a regular git repository.

You can pack the previous repos running this command:
```
borges pack --file=repos.txt --to=/home/me/packed-repos
```

With the `--to` argument you can specify where you want the siva files stored. If the directory does not exist it will be created. If you omit this argument siva files will be stored in `$PWD/repositories` by default.

For more detauls, use `borges pack -h`


## Administration Notes

Both the producer and consumer services will run even if they cannot connect to
the queue, or even if the queue is malfunctioning. If the queue does not work,
they will just retry until it does.

# Quickstart using docker containers

## Download the images

Download the latest borges image

```
docker pull quay.io/srcd/borges
```

And then the PostgreSQL and RabbitMQ images (you can skip this step if you already have that setup for [rovers](https://github.com/src-d/rovers)).

```
docker pull postgres:9.6-alpine
docker pull rabbitmq:3-management
```

## Running everything

Start RabbitMQ and PostgreSQL (you can skip this step if you already have that setup for [rovers](https://github.com/src-d/rovers).

```
docker run -d --name postgres -e POSTGRES_PASSWORD=testing -p 5432:5432 -e POSTGRES_USER=testing postgres
docker run -d --hostname rabbitmq --name rabbitmq -p 8081:15672 -p 5672:5672 rabbitmq:3-management
```

Now, you can start the borges consumer, the component that will be listening for jobs and processing repositories.

```
docker run --name borges_consumer --link rabbitmq --link postgres \
        -v /path/to/store/repos/locally:/var/root-repositories \
        quay.io/srcd/borges /bin/sh -c "borges init; borges consumer --workers=1"
```

Be sure to replace `/path/to/store/repos/locally` with the path on your hard drive where you want your root repositories (as siva files) stored.

Finally, you need to send jobs to the borges consumer using the borges producer. If you have [rovers](https://github.com/src-d/rovers) setup already, you may want to use the rovers' mentions as the source.

```
docker run --name borges_consumer --link rabbitmq --link postgres \
        quay.io/srcd/borges borges producer mentions
```

However, you can also process just a specific list of repositories without having to setup rovers on your own. Write the repository URLs in a file, one repository per line and feed it to the borges producer with the `file` source. (This example assumes you have a `repos.txt` in the current directory).

```
docker run --name borges_consumer_file --link rabbitmq --link postgres \
        -e $(pwd):/opt/borges
        quay.io/srcd/borges borges producer file /opt/borges/repos.txt
```

Congratulations, now you have a fully working repository processing pipeline!

**Note:** remember you can configure borges using environment variables as described in previous sections by using the `-e` flag of docker, e.g `-e CONFIG_DBHOST=foo`.

# Running Borges in Kubernetes

You can use the official [Helm](https://github.com/kubernetes/helm) [chart](https://github.com/src-d/charts/tree/master/borges) to deploy Borges in your kubernetes cluster.

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
    docker run -d --hostname rabbit --name rabbit -p 8080:15672 -p 5672:5672 rabbitmq:3-management
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

## License

GPLv3, see [LICENSE](LICENSE)
