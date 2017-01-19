
borges
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
