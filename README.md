# BRAD

"Single-interface" version

## Getting Started

### Setting up BRAD

Run `./tools/install-dev.sh` to install BRAD in development mode. This script
will also take care of installing BRAD's Python dependencies.

If you would like to install BRAD in a `virtualenv`, run
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

### Creating a Configuration File

Make a copy of `config/config_sample.yml` and fill in the configurations using
your values. Make sure you **do not** check in your configuration file, as it
will contain your AWS access keys.

### Creating Tables

BRAD expects to be given your schema up front. You must also use the BRAD
administrative tools `brad admin` to bootstrap the tables across the underlying
engines.

- Create a schema file (see `config/schemas/test_schema.yml` for an example). Use
  PostgreSQL data types. Remember the schema name that you choose; you will need
  it when starting the BRAD server.
- Run `brad admin bootstrap_schema --config-file path/to/config.yml
  --schema-file path/to/your/schema.yml` to set up the tables across the
  underlying engines.
- To bulk load data, upload your data to S3 and then create a manifest file (see
  `config/manifests/manifest_sample.yml` for an example). Then run `brad admin
  bulk_load --config-file path/to/config.yml --manifest-file
  path/to/manifest.yml` to execute the bulk load (this time may take some time,
  depending on how much data you are loading).
- Start the BRAD server `brad server --config-file path/to/config.yml
  --schema-name your_schema_name --planner-config-file path/to/planner.yml`.
- Run queries through the CLI `brad cli`.

To remove the tables, use `brad admin drop_schema` (e.g., `brad admin
drop_schema --config-file path/to/config.yml --schema-name
your_schema_name`). Note that this command will delete the data in the tables
(and will drop the tables)!


### Upgrade Steps

This section documents manual update steps you will need to take if you were
running an older version of BRAD and have existing configurations and legacy
files.

- (06/22/2023) Rename the `s3_metadata_bucket` and `s3_metadata_path` keys in
  the configuration to `s3_assets_bucket` and `s3_assets_path` respectively.
- (06/30/2023) If you have already created the IMDB schema, run `brad admin
  modify_blueprint --add-indexes --schema-file config/schemas/imdb.yml
  --config-file path/to/your/config.yml` to add indexes to your deployment.


### Generate IMDB workload

Required package (pip install): pandas, absl-py, numpy

Load schema and data: TODO

Generate_workload: 

The hyperparameters with documentation can be found at `workloads/IMDB/parameters.py`. 
Please correct the path if necessary. 
You can create a super class of `WorkloadParams` to explore different parameters, 
and make sure to put `@Register` before your parameter class.

```angular2html
python --run Default
```


### Working with the HATtrick benchmark

Clone the HATtrick repository to get the HATtrick benchmark tools:

https://github.com/UWHustle/HATtrick

To generate data:
- Modify the `UserInput.cpp` file to set the scale factor (`SF`) and data
  delimiter (our scripts are configured for `|`).
- Compile the tools: `make all`
- Run `./HATtrickBench -gen -pa output_dir` to generate the data.

Copy the data to S3 and use the schema and manifest files under `config` to
bootstrap and bulk load the data into BRAD respectively.
