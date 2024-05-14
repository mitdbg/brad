CREATE INDEX theatres_name_idx ON theatres (name);

CREATE INDEX showings_theatre_id_idx ON showings (theatre_id);
CREATE INDEX showings_movie_id_idx ON showings (movie_id);
CREATE INDEX showings_theatre_id_date_time_idx ON showings (theatre_id, date_time);

CREATE INDEX ticket_orders_showing_id_idx ON ticket_orders (showing_id);

CREATE INDEX aka_name_person_id_idx ON aka_name (person_id);

CREATE INDEX aka_title_movie_id_idx ON aka_title (movie_id);
CREATE INDEX aka_title_kind_id_idx ON aka_title (kind_id);

CREATE INDEX cast_info_person_id_idx ON cast_info (person_id);
CREATE INDEX cast_info_movie_id_idx ON cast_info (movie_id);
CREATE INDEX cast_info_person_role_id_idx ON cast_info (person_role_id);

CREATE INDEX char_name_imdb_id_idx ON char_name (imdb_id);

CREATE INDEX company_name_imdb_id_idx ON company_name (imdb_id);

CREATE INDEX complete_cast_movie_id_idx ON complete_cast (movie_id);
CREATE INDEX complete_cast_subject_id_idx ON complete_cast (subject_id);
CREATE INDEX complete_cast_status_id_idx ON complete_cast (status_id);

CREATE INDEX movie_companies_movie_id_idx ON movie_companies (movie_id);
CREATE INDEX movie_companies_company_id_idx ON movie_companies (company_id);
CREATE INDEX movie_companies_company_type_id_idx ON movie_companies (company_type_id);

CREATE INDEX movie_info_idx_movie_id_idx ON movie_info_idx (movie_id);
CREATE INDEX movie_info_idx_info_type_id_idx ON movie_info_idx (info_type_id);

CREATE INDEX movie_keyword_movie_id_idx ON movie_keyword (movie_id);
CREATE INDEX movie_keyword_keyword_id_idx ON movie_keyword (keyword_id);

CREATE INDEX movie_link_movie_id_idx ON movie_link (movie_id);
CREATE INDEX movie_link_linked_movie_id_idx ON movie_link (linked_movie_id);
CREATE INDEX movie_link_link_type_id_idx ON movie_link (link_type_id);

CREATE INDEX name_imdb_id_idx ON name (imdb_id);

CREATE INDEX title_kind_id_idx ON title (kind_id);
CREATE INDEX title_imdb_id_idx ON title (imdb_id);
CREATE INDEX title_episode_of_id_idx ON title (episode_of_id);

CREATE INDEX movie_info_movie_id_idx ON movie_info (movie_id);
CREATE INDEX movie_info_info_type_id_idx ON movie_info (info_type_id);

CREATE INDEX person_info_person_id_idx ON person_info (person_id);
CREATE INDEX person_info_info_type_id_idx ON person_info (info_type_id);