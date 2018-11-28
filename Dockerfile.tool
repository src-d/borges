FROM debian:stretch-slim
MAINTAINER jfontan

RUN apt-get update && \
    apt-get install -y glusterfs-common && \
    apt-get autoremove -y

ADD build/bin/borges-tool /bin/

CMD ["bash"]
