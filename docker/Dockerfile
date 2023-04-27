FROM ubuntu:22.04

RUN apt update --fix-missing && \
  apt install --no-install-recommends -y software-properties-common && \
  apt update && \
  apt install --no-install-recommends -y sudo && \
  apt install --no-install-recommends -y wget bzip2 ca-certificates libssl-dev dirmngr gpg-agent gnupg && \
  apt install --no-install-recommends -y python3-pip python3-setuptools python3-dev python3.11-venv && \
  apt install --no-install-recommends -y unixodbc-dev odbc-postgresql alien && \
  rm -rf /var/lib/apt/lists/*

# Install the Redshift ODBC driver.
RUN wget "https://s3.amazonaws.com/redshift-downloads/drivers/odbc/1.4.65.1000/AmazonRedshiftODBC-64-bit-1.4.65.1000-1.x86_64.deb" -O /opt/redshift_odbc.deb && \
  cd /opt && apt install ./redshift_odbc.deb

# Install the Athena ODBC driver.
RUN wget "https://s3.amazonaws.com/athena-downloads/drivers/ODBC/SimbaAthenaODBC_1.1.19.1002/Linux/simbaathena-1.1.19.1002-1.el7.x86_64.rpm" -O /opt/athena_odbc.rpm && \
  cd /opt && alien -i athena_odbc.rpm

# NOTE: gosu is used in create-user.sh
RUN mkdir ~/.gnupg && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf
RUN gpg --keyserver keyserver.ubuntu.com --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4
RUN wget "https://github.com/tianon/gosu/releases/download/1.11/gosu-$(dpkg --print-architecture | awk -F- '{ print $NF }')" -O /usr/local/bin/gosu && \
  wget "https://github.com/tianon/gosu/releases/download/1.11/gosu-$(dpkg --print-architecture | awk -F- '{ print $NF }').asc" -O /usr/local/bin/gosu.asc && \
  gpg --verify /usr/local/bin/gosu.asc && \
  rm /usr/local/bin/gosu.asc && \
  chmod +x /usr/local/bin/gosu

COPY create-user.sh /usr/local/bin/create-user.sh
RUN chmod +x /usr/local/bin/create-user.sh
ENTRYPOINT ["/usr/local/bin/create-user.sh"]
CMD ["/bin/bash"]
