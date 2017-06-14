FROM quay.io/srcd/basic:latest
MAINTAINER source{d}

ADD bin /bin

CMD ["borges"]

