SELECT MIN("char_name"."id") as agg_0 FROM "char_name" LEFT OUTER JOIN "cast_info" ON "char_name"."id" = "cast_info"."person_role_id" LEFT OUTER JOIN "title" ON "cast_info"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_companies" ON "title"."id" = "movie_companies"."movie_id" LEFT OUTER JOIN "aka_name" ON "cast_info"."person_id" = "aka_name"."id"  WHERE "title"."title" NOT LIKE '%of%' AND "title"."imdb_index" != '%XVIII%' AND "char_name"."surname_pcode" LIKE '%M%5%' AND "cast_info"."movie_id" >= 112942 AND "movie_companies"."movie_id" IS NOT NULL AND "aka_name"."id" IS NOT NULL;