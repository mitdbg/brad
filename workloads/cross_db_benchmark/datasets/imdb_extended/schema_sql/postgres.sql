DROP TABLE IF EXISTS homes;
CREATE TABLE homes (
    id SERIAL PRIMARY KEY,
    location_x DECIMAL(10),
    location_y DECIMAL(10)
);

DROP TABLE IF EXISTS theatres;
CREATE TABLE theatres (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256),
    location_x DECIMAL(10),
    location_y DECIMAL(10)
);

CREATE INDEX theatres_name_idx ON theatres (name);

DROP TABLE IF EXISTS showings;
CREATE TABLE showings (
    id SERIAL PRIMARY KEY,
    theatre_id BIGINT,
    movie_id BIGINT,
    date_time TIMESTAMP,
    total_capacity INT,
    seats_left INT
);

CREATE INDEX showings_theatre_id_idx ON showings (theatre_id);
CREATE INDEX showings_movie_id_idx ON showings (movie_id);
CREATE INDEX showings_theatre_id_date_time_idx ON showings (theatre_id, date_time);

DROP TABLE IF EXISTS ticket_orders;
CREATE TABLE ticket_orders (
    id SERIAL PRIMARY KEY,
    showing_id BIGINT,
    quantity INT,
    contact_name TEXT,
    location_x DECIMAL(10),
    location_y DECIMAL(10)
);

CREATE INDEX ticket_orders_showing_id_idx ON ticket_orders (showing_id);

DROP TABLE IF EXISTS aka_name;
CREATE TABLE aka_name (
    id SERIAL PRIMARY KEY,
    person_id BIGINT,
    name TEXT,
    imdb_index CHARACTER VARYING(3),
    name_pcode_cf CHARACTER VARYING(11),
    name_pcode_nf CHARACTER VARYING(11),
    surname_pcode CHARACTER VARYING(11),
    md5sum CHARACTER VARYING(65)
);

CREATE INDEX aka_name_person_id_idx ON aka_name (person_id);

DROP TABLE IF EXISTS aka_title;
CREATE TABLE aka_title (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    title TEXT,
    imdb_index CHARACTER VARYING(4),
    kind_id BIGINT,
    production_year BIGINT,
    phonetic_code CHARACTER VARYING(5),
    episode_of_id BIGINT,
    season_nr BIGINT,
    episode_nr BIGINT,
    note CHARACTER VARYING(72),
    md5sum CHARACTER VARYING(32)
);

CREATE INDEX aka_title_movie_id_idx ON aka_title (movie_id);
CREATE INDEX aka_title_kind_id_idx ON aka_title (kind_id);

DROP TABLE IF EXISTS cast_info;
CREATE TABLE cast_info (
    id SERIAL PRIMARY KEY,
    person_id BIGINT,
    movie_id BIGINT,
    person_role_id BIGINT,
    note TEXT,
    nr_order BIGINT,
    role_id BIGINT
);

CREATE INDEX cast_info_person_id_idx ON cast_info (person_id);
CREATE INDEX cast_info_movie_id_idx ON cast_info (movie_id);
CREATE INDEX cast_info_person_role_id_idx ON cast_info (person_role_id);

DROP TABLE IF EXISTS char_name;
CREATE TABLE char_name (
    id SERIAL PRIMARY KEY,
    name TEXT,
    imdb_index CHARACTER VARYING(2),
    imdb_id BIGINT,
    name_pcode_nf CHARACTER VARYING(5),
    surname_pcode CHARACTER VARYING(5),
    md5sum CHARACTER VARYING(32)
);

CREATE INDEX char_name_imdb_id_idx ON char_name (imdb_id);

DROP TABLE IF EXISTS comp_cast_type;
CREATE TABLE comp_cast_type (
    id SERIAL PRIMARY KEY,
    kind CHARACTER VARYING(32)
);

DROP TABLE IF EXISTS company_name;
CREATE TABLE company_name (
    id SERIAL PRIMARY KEY,
    name TEXT,
    country_code CHARACTER VARYING(6),
    imdb_id BIGINT,
    name_pcode_nf CHARACTER VARYING(5),
    name_pcode_sf CHARACTER VARYING(5),
    md5sum CHARACTER VARYING(32)
);

CREATE INDEX company_name_imdb_id_idx ON company_name (imdb_id);

DROP TABLE IF EXISTS company_type;
CREATE TABLE company_type (
    id SERIAL PRIMARY KEY,
    kind CHARACTER VARYING(32)
);

DROP TABLE IF EXISTS complete_cast;
CREATE TABLE complete_cast (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    subject_id BIGINT,
    status_id BIGINT
);

CREATE INDEX complete_cast_movie_id_idx ON complete_cast (movie_id);
CREATE INDEX complete_cast_subject_id_idx ON complete_cast (subject_id);
CREATE INDEX complete_cast_status_id_idx ON complete_cast (status_id);

DROP TABLE IF EXISTS info_type;
CREATE TABLE info_type (
    id SERIAL PRIMARY KEY,
    info CHARACTER VARYING(32)
);

DROP TABLE IF EXISTS keyword;
CREATE TABLE keyword (
    id SERIAL PRIMARY KEY,
    keyword TEXT,
    phonetic_code CHARACTER VARYING(5)
);

DROP TABLE IF EXISTS kind_type;
CREATE TABLE kind_type (
    id SERIAL PRIMARY KEY,
    kind CHARACTER VARYING(15)
);

DROP TABLE IF EXISTS link_type;
CREATE TABLE link_type (
    id SERIAL PRIMARY KEY,
    link CHARACTER VARYING(32)
);

DROP TABLE IF EXISTS movie_companies;
CREATE TABLE movie_companies (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    company_id BIGINT,
    company_type_id BIGINT,
    note TEXT
);

CREATE INDEX movie_companies_movie_id_idx ON movie_companies (movie_id);
CREATE INDEX movie_companies_company_id_idx ON movie_companies (company_id);
CREATE INDEX movie_companies_company_type_id_idx ON movie_companies (company_type_id);

DROP TABLE IF EXISTS movie_info_idx;
CREATE TABLE movie_info_idx (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    info_type_id BIGINT,
    info TEXT,
    note CHARACTER VARYING(1)
);

CREATE INDEX movie_info_idx_movie_id_idx ON movie_info_idx (movie_id);
CREATE INDEX movie_info_idx_info_type_id_idx ON movie_info_idx (info_type_id);

DROP TABLE IF EXISTS movie_keyword;
CREATE TABLE movie_keyword (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    keyword_id BIGINT
);

CREATE INDEX movie_keyword_movie_id_idx ON movie_keyword (movie_id);
CREATE INDEX movie_keyword_keyword_id_idx ON movie_keyword (keyword_id);

DROP TABLE IF EXISTS movie_link;
CREATE TABLE movie_link (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    linked_movie_id BIGINT,
    link_type_id BIGINT
);

CREATE INDEX movie_link_movie_id_idx ON movie_link (movie_id);
CREATE INDEX movie_link_linked_movie_id_idx ON movie_link (linked_movie_id);
CREATE INDEX movie_link_link_type_id_idx ON movie_link (link_type_id);

DROP TABLE IF EXISTS name;
CREATE TABLE name (
    id SERIAL PRIMARY KEY,
    name TEXT,
    imdb_index CHARACTER VARYING(9),
    imdb_id BIGINT,
    gender CHARACTER VARYING(1),
    name_pcode_cf CHARACTER VARYING(5),
    name_pcode_nf CHARACTER VARYING(5),
    surname_pcode CHARACTER VARYING(5),
    md5sum CHARACTER VARYING(32)
);

CREATE INDEX name_imdb_id_idx ON name (imdb_id);

DROP TABLE IF EXISTS role_type;
CREATE TABLE role_type (
    id SERIAL PRIMARY KEY,
    role CHARACTER VARYING(32)
);

DROP TABLE IF EXISTS title;
CREATE TABLE title (
    id SERIAL PRIMARY KEY,
    title TEXT,
    imdb_index CHARACTER VARYING(5),
    kind_id BIGINT,
    production_year BIGINT,
    imdb_id BIGINT,
    phonetic_code CHARACTER VARYING(5),
    episode_of_id BIGINT,
    season_nr BIGINT,
    episode_nr BIGINT,
    series_years CHARACTER VARYING(49),
    md5sum CHARACTER VARYING(32)
);

CREATE INDEX title_kind_id_idx ON title (kind_id);
CREATE INDEX title_imdb_id_idx ON title (imdb_id);
CREATE INDEX title_episode_of_id_idx ON title (episode_of_id);

DROP TABLE IF EXISTS movie_info;
CREATE TABLE movie_info (
    id SERIAL PRIMARY KEY,
    movie_id BIGINT,
    info_type_id BIGINT,
    info TEXT,
    note TEXT
);

CREATE INDEX movie_info_movie_id_idx ON movie_info (movie_id);
CREATE INDEX movie_info_info_type_id_idx ON movie_info (info_type_id);

DROP TABLE IF EXISTS person_info;
CREATE TABLE person_info (
    id SERIAL PRIMARY KEY,
    person_id BIGINT,
    info_type_id BIGINT,
    info TEXT,
    note TEXT
);

CREATE INDEX person_info_person_id_idx ON person_info (person_id);
CREATE INDEX person_info_info_type_id_idx ON person_info (info_type_id);

