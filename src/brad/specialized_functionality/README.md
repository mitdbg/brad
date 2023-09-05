# Specialized functionality

This directory contains helper functions that help BRAD run queries with specialized functionality (like geospatial queries).

## Geospatial queries

In `QueryRep`, we determine whether a query makes use of geospatial functions by determining whether PostGIS keywords appear in the query. `geospatial_keywords.yml` contains a list of the PostGIS keywords that BRAD considers. `geospatial_keywords.yml` can be updated by running `python geospatial_keywords.py`, which crawls a list of PostGIS keywords from [PostGIS' specialized functions index](https://postgis.net/docs/manual-1.5/ch08.html).

Crawling the PostGIS documentation requires `requests` and `bs4`.
