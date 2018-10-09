# Getting started

## Quickstart using docker containers

### Download the images

Download the latest borges image

```
docker pull srcd/borges
```

And then the PostgreSQL and RabbitMQ images (you can skip this step if you already have that setup for [rovers](https://github.com/src-d/rovers)).

```
docker pull postgres:9.6-alpine
docker pull rabbitmq:3-management
```

### Running everything

Start RabbitMQ and PostgreSQL (you can skip this step if you already have that setup for [rovers](https://github.com/src-d/rovers)).

```
docker run -d --name postgres -e POSTGRES_PASSWORD=testing -p 5432:5432 -e POSTGRES_USER=testing postgres
docker run -d --hostname rabbitmq --name rabbitmq -p 8081:15672 -p 5672:5672 rabbitmq:3-management
```

Now, you can start the borges consumer, the component that will be listening for jobs and processing repositories.

```
docker run --name borges_consumer --link rabbitmq --link postgres \
        -v /path/to/store/repos/locally:/var/root-repositories \
        srcd/borges /bin/sh -c "borges init; borges consumer --workers=1"
```

Be sure to replace `/path/to/store/repos/locally` with the path on your hard drive where you want your root repositories (as siva files) stored.

Finally, you need to send jobs to the borges consumer using the borges producer. If you have [rovers](https://github.com/src-d/rovers) setup already, you may want to use the rovers' mentions as the source.

```
docker run --name borges_producer --link rabbitmq --link postgres \
        srcd/borges borges producer mentions
```

However, you can also process just a specific list of repositories without having to setup rovers on your own. Write the repository URLs in a file, one repository per line and feed it to the borges producer with the `file` source. (This example assumes you have a `repos.txt` in the current directory).

```
docker run --name borges_producer_file --link rabbitmq --link postgres \
        -v $(pwd):/opt/borges \
        srcd/borges borges producer file /opt/borges/repos.txt
```

Congratulations, now you have a fully working repository processing pipeline!

**Note:** remember you can configure borges using environment variables as described in previous sections by using the `-e` flag of docker, e.g `-e BORGES_DATABASE=postgres://testing:testing@localhost/testing`.

## Running Borges in Kubernetes

You can use the official [Helm](https://github.com/kubernetes/helm) [chart](https://github.com/src-d/charts/tree/master/borges) to deploy Borges in your kubernetes cluster.
