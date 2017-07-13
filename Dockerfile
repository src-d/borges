FROM alpine:3.6
MAINTAINER source{d}

RUN apk add --no-cache ca-certificates

ADD build/borges_linux_amd64/borges /bin/

CMD ["borges"]
