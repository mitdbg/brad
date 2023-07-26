# Geospatial workload extending IMDB

## Installing PostGIS on Aurora

See [this tutorial](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html).

## Relevant tables

Tables touched by geospatial queries:

- `theatres(id*, name, location_x, location_y)`
- `ticket_orders(id*, showing_id, quantity, contact_name, location_x, location_y)`
- `showings(id*, theatre_id, movie_id, date_time, total_capacity, seats_left)`
- `homes(id*, location_x, location_y)`

`homes` is additionally added for this workload.

## Queries

- `query1`: For each cinema, count how many homes are within certain proximity
- `query2`: Select homes with many cinemas in certain proximity
