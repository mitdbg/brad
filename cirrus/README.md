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
