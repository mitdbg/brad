# Geospatial workload extending IMDB

## Installing PostGIS on Aurora

See [this tutorial](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html).

## Relevant tables

- `theatres(id*, name, location_x, location_y)`: Location of theatres
- `homes(id*, location_x, location_y)`: Homes of users that buy tickets

`homes` is additionally added for this workload.

## Queries

- `query1`: For each cinema, count how many homes are within certain proximity
- `query2`: Select homes with many cinemas in certain proximity
