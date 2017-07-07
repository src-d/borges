FROM alpine:3.6
MAINTAINER source{d}

ADD build/borges_linux_amd64/rovers /bin/

CMD ["borges"]
