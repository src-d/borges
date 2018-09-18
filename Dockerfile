FROM alpine:3.6
MAINTAINER source{d}

ENV BORGES_DATABASE=postgres://testing:testing@postgres/testing?application_name=borges&sslmode=disable&connect_timeout=30
ENV BORGES_BROKER=amqp://guest:guest@rabbitmq:5672/
ENV BORGES_ROOT_REPOSITORIES_DIR=/var/root-repositories

RUN mkdir -p /var/root-repositories

RUN apk add --no-cache ca-certificates dumb-init=1.2.0-r0 git

ADD build/borges_linux_amd64/borges /bin/

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["borges"]
