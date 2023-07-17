# Transactions on the IMDB schema

## On top of the existing schema

### Edit movie note transaction

- Select a movie (title table), read its id
- Read matching rows in
  - movie_info
  - aka_title
- Edit note (append characters or remove them) in
  - movie_info
  - aka_title

## On top of new tables added to the schema

- `theatre(id*, name, location_x, location_y)`
  - Represents movie theatres
- `showing(id*, theatre_id, date_time, movie_id, total_capacity, seats_left)`
  - Represents a theatre showing a particular movie at a date and time
  - `seats_left` is decremented whenever someone purchases a ticket
- `ticket_order(id*, showing_id, quantity, contact_name, location_x, location_y)`
  - Represents ticket orders.
  - Each order can be of multiple tickets (hence the `quantity` column)

### Add new showings

- Select a theatre, select a movie, select a date in the future
- Insert into showing

### Purchase tickets

- Select theatre by name or id
- Select showing by theatre id and date
- Insert into `ticket_order`
- Update the `showing` entry


## Other extension opportunities

- Geospatial analytical queries over `ticket_order` locations and `theatre`
  locations
- Finding nearby movie theatres
