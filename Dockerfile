FROM quay.io/srcd/basic:latest
MAINTAINER source{d}

ADD borges /bin

CMD ["borges","consumer"]