ARG codename
FROM ubuntu:${codename:-bionic}

ARG codename
ENV CODE_NAME=${codename:-bionic}

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Moskow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo build-essential \
    gcc lsb-release libssl-dev gnupg openssl \
    gdb git curl

RUN curl -s 'http://keyserver.ubuntu.com/pks/lookup?op=get&search=0xafc3ce0d00e3c45a357e9e637fcd11186050cd1a' | \
    gpg --dearmour -o /etc/apt/trusted.gpg.d/yandex.gpg

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo build-essential \
    gcc lsb-release libssl-dev gnupg openssl \
    gdb git \
    libpam0g-dev \
    debhelper debootstrap devscripts make equivs debhelper-compat \
    libz-dev flex libicu-dev libio-pty-perl libipc-run-perl libkrb5-dev \
    libldap2-dev liblz4-dev liblz4-tool zstd libperl-dev libreadline-dev libselinux1-dev llvm-dev \
    libsystemd-dev libxml2-dev libxml2-utils libxslt1-dev \
    pkg-config python3-dev systemtap-sdt-dev tcl-dev uuid-dev xsltproc zlib1g-dev \
    bison dh-exec docbook-xml docbook-xsl

RUN groupadd -g 999 build-user && \
    useradd -r -u 999 -g build-user build-user

COPY . /home/build-user
RUN chown build-user:build-user /home -R && usermod -aG sudo build-user

RUN echo 'build-user ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER build-user
WORKDIR /home/build-user

RUN CFLAGS="" ./configure --prefix=/home/build-user/pgbin  --without-mdblocales  --enable-depend --enable-cassert --enable-debug --enable-tap-tests && \
    make -j8 && \
    sudo make install
