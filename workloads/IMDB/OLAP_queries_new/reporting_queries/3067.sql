SELECT MIN("title"."episode_of_id") as agg_0 FROM "company_name" LEFT OUTER JOIN "movie_companies" ON "company_name"."id" = "movie_companies"."company_id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "movie_info_idx" ON "title"."id" = "movie_info_idx"."movie_id" LEFT OUTER JOIN "info_type" ON "movie_info_idx"."info_type_id" = "info_type"."id" LEFT OUTER JOIN "company_type" ON "movie_companies"."company_type_id" = "company_type"."id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "cast_info" ON "title"."id" = "cast_info"."movie_id" LEFT OUTER JOIN "char_name" ON "cast_info"."person_role_id" = "char_name"."id"  WHERE ("movie_info_idx"."id" BETWEEN 1156916 AND 1306837 OR ("movie_info_idx"."id" >= 362986 AND "movie_info_idx"."id" BETWEEN 197829 AND 1045645)) AND ("movie_info"."info" NOT LIKE '%Japanese%' OR "movie_info"."info_type_id" <= 9 OR "movie_info"."info" LIKE '%G%erman%') AND "company_type"."kind" NOT LIKE '%companies%' AND ("title"."series_years" != '%2008-2011%' OR "title"."series_years" != '%1990-2003%' OR "title"."series_years" IN ('2011-????', '2008-????', '2009-????', '2005-????', '2004-????', '2010-????', '2012-????', '2006-????')) AND "char_name"."imdb_index" != '%II%' AND "movie_companies"."note" NOT LIKE '%(worldwide)%' AND "info_type"."id" IS NOT NULL AND "kind_type"."id" IS NOT NULL;