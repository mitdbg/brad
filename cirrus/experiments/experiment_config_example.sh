RDS_HOST="redshift-test.amazonaws.com"
RDS_USER="awsuser"
RDS_DBNAME="dev"
# What this means:
# Put your user password into an environment variable called RPWD. This is used
# to avoid passing your password directly as a command line argument.
RDS_PWDVAR="RPWD"

# Aurora host needs to be specified in `~/.odbc.ini`.
AUR_USER="postgres"
AUR_PWDVAR="RPWD"

# ODBC config
PG_ODBC_DSN="RDS PostgreSQL"
PG_ODBC_USER="postgres"
PWDVAR="DBPWD"
