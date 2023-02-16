# IOHTAP

"Single-interface" version

## Getting Started

### Setting up IOHTAP

Run `./tools/install-dev.sh` to install IOHTAP in development mode. This script
will also take care of installing IOHTAP's Python dependencies.

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
  - You will use this driver to connect to Aurora PostgreSQL
- **Athena**
  - Download the 64-bit Linux driver: https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html
  - Install `alien`: `sudo apt install alien` (it converts `*.rpm` files into `*.deb` files for installation on Ubuntu)
  - Install the driver: `sudo alien -i path/to/downloaded/athena_driver.rpm`
  - Add the following snippet to `~/.odbcinst.ini`
    ```ini
    [Athena]
    Description=Amazon Athena Driver
    Driver=/opt/simba/athenaodbc/lib/64/libathenaodbc_sb64.so
    ```

### Creating Tables

IOHTAP expects to be given your schema up front. You must also use the IOHTAP
administrative tools `iohtap admin` to set up the tables across all the
underlying engines.

- Create a schema file (see `config/test_schema.yml` for an example). Use
  PostgreSQL data types.
- Run `iohtap admin set_up_tables --config-file path/to/config.yml --schema-file
  path/to/your/schema.yml` to set up the tables across the underlying engines.
- Start the IOHTAP server `iohtap server --config-file path/to/config.yml --schema-file path/to/schema.yml`.
- Run queries through the CLI `iohtap cli`.

To remove the tables, use `iohtap admin tear_down_tables` (e.g., `iohtap admin
tear_down_tables --config-file path/to/config.yml --schema-file
path/to/your/schema.yml`). Note that this command will delete the data in the
tables (and will drop the tables)!
