# Configuration

## CLI

Both producer and consumer are shipped as a single binary,
see `borges --help` to get details about the main commands and their options.

## Setting up borges

Borges needs a PostgreSQL database and RabbitMQ as a message broker to do its job.
You can configure the database parameters setting a connection string with parameter `--database` or with environment variable `BORGES_DATABASE`. By default this connection string is `postgres://testing:testing@0.0.0.0:5432/testing?application_name=borges&sslmode=disable&connect_timeout=30`:

* user: `testing`
* password: `testing`
* host: `0.0.0.0`
* port: `5432`
* name: `testing`
* application name: `borges`
* ssl mode: `disable`
* timeout in seconds: `30`

Other important settings are (`--parameter`/`ENVIRONMENT_VARIABLE`):

* `--queue`/`BORGES_QUEUE`: AMQP queue name, by default: `borges`.
* `--broker`/`BORGES_BROKER`: Broker service URI, by default: `amqp://localhost:5672`.
* `--locking`/`BORGES_LOCKING`: Locking service configuration, by default: `local:`, other option: `etcd:<connection string>`.
* `--workers`/`BORGES_WORKERS`: Number of workers, by default: `1`, `0` means the same number as processors.
* `--timeout`/`BORGES_TIMEOUT`: Deadline to process a job, by default: `10h`.
* `--root-repositories-dir`/`BORGES_ROOT_REPOSITORIES_DIR`: Path to the directory storing rooted repositories (can be local path or `hdfs://`), by default: `/tmp/root-repositories`.
* `--bucket-size`/`BORGES_BUCKETSIZE`: Number of characters used from the siva file name to create bucket directories. The value `0` means that all files will be saved at the same level, by default: `0`.
* `--temp-dir`/`BORGES_TEMP_DIR`: Local path to store temporal files needed by the Borges consumer, by default: `/tmp/sourced`.
* `--temp-dir-clean`/`BORGES_TEMP_DIR_CLEAN`: Delete temporary directory before starting, by default: `false`
* `--log-level`/`LOG_LEVEL`: Minimum log level that is printed, by default: `info`.
* `--log-format`/`LOG_FORMAT`: Format to print logs (`text` or `json`), by default: `text` on a terminal or `json` otherwise.
* `--log-fields`/`LOG_FIELDS`: Default fields for the logger specified in json.
* `--log-force-format`/`LOG_FORCE_FORMAT`: Ignore if it is running on a terminal or not.

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

You can change the priority of jobs produced with `--queue-priority` option. It is a number from 0 to 8 where 0 is the lowest priority:

    borges producer file --queue-priority 8 /path/to/file

When jobs fail they're sent to the buried queue. If you want to requeue them, you can pass the `--republish-buried` flag (this only works for the `mentions` source). For example:

```
borges producer --republish-buried
```

So a possible command to launch the producer could be:

```bash
$ BORGES_DATABASE="postgres://user:password@localhost/borges-db" \
BORGES_BROKER="amqp://guest:guest@rabbitmq:5672" \
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

You can select the number of workers to use, by default it uses 1:

    borges consumer --workers=4

A command you could use to run it could be:

```bash
$ BORGES_TEMP_DIR="/borges/tmp"  \
BORGES_ROOT_REPOSITORIES_DIR="/borges/root-repositories"  \
LOG_LEVEL=debug \
borges consumer --workers=4
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
borges pack --root-repositories-dir=/home/me/packed-repos repos.txt
```

With the `--root-repositories-dir` argument you can specify where you want the siva files stored. If the directory does not exist it will be created. If you omit this argument siva files will be stored in `$PWD/repositories` by default.

For more defaults, use `borges pack -h`


## Administration Notes

Both the producer and consumer services will run even if they cannot connect to
the queue, or even if the queue is malfunctioning. If the queue does not work,
they will just retry until it does.