# Data orchestration project (Cirrus)

The code in this folder is used to run motivational experiments for the derived
state (now "data orchestrator") project.


## Setting up ODBC to run against Redshift

Instructions are written for a Debian based machine (e.g., Ubuntu).

- Install unixODBC: `sudo apt install unixodbc-dev`
- Install the Redshift ODBC driver (you will want the 64-bit driver)
  https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html#install-odbc-driver-linux
- You may need to add `.odbc.ini` and `.odbcinst.ini` to your home directory.
  Follow the instructions here:
  https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html#odbc-driver-configure-linux-mac


## Setting up ODBC for PostgreSQL

- Make sure unixODBC is installed: `sudo apt install unixodbc-dev`
- Install `odbc-postgresql`: `sudo apt install odbc-postgresql`
- The driver should be installed to `/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so`
- We use `nanodbc` to avoid dealing with the native C ODBC API. But there
  appears to be a strange bug where the library (either `nanodbc` or the native
  ODBC library itself) ignores the connection string and tries to connect to a
  local PostgreSQL instance instead.
- The workaround is you need to define a custom data source in `~/.odbc.ini` and
  ask `nanodbc` to connect using that data source instead.

```ini
[Aurora]
Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
Servername=aurora-postgres-1.cluster-cwnnmm0augmy.us-east-1.rds.amazonaws.com
Port=5432
```
