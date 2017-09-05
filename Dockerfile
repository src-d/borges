FROM alpine:3.6
MAINTAINER source{d}

RUN apk add --no-cache ca-certificates dumb-init=1.2.0-r0

ADD build/borges_linux_amd64/borges /bin/

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["borges"]
