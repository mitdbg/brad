# IOHTAP

"Single-interface" version

## Getting Started

### Setting up IOHTAP

Run `./tools/install-dev.sh` to install IOHTAP in development mode. This script
will also take care of installing IOHTAP's dependencies.

If you would like to install IOHTAP in a `virtualenv`, run
`./tools/install-dev.sh --virtualenv <venv name>` instead.

### Installing the ODBC drivers

Note that these instructions are written for a Debian based machine (e.g., Ubuntu).

- Install unixODBC: `sudo apt install unixodbc-dev`
- **Redshift**:
  - Install the Redshift ODBC driver (you will want the 64-bit driver)
    https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html#install-odbc-driver-linux
  - You may need to add `.odbc.ini` and `.odbcinst.ini` to your home directory.
    Follow the instructions here:
    https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html#odbc-driver-configure-linux-mac
- **PostgreSQL**:
  - Install `odbc-postgresql`: `sudo apt install odbc-postgresql`
  - The driver should be installed to `/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so`
