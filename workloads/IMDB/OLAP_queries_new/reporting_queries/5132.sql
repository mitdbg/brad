SELECT MIN("title"."production_year") as agg_0, MIN("cast_info"."person_role_id") as agg_1 FROM "info_type" LEFT OUTER JOIN "movie_info_idx" ON "info_type"."id" = "movie_info_idx"."info_type_id" LEFT OUTER JOIN "title" ON "movie_info_idx"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "movie_keyword" ON "title"."id" = "movie_keyword"."movie_id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "keyword" ON "movie_keyword"."keyword_id" = "keyword"."id" LEFT OUTER JOIN "cast_info" ON "title"."id" = "cast_info"."movie_id" LEFT OUTER JOIN "aka_name" ON "cast_info"."person_id" = "aka_name"."id" LEFT OUTER JOIN "char_name" ON "cast_info"."person_role_id" = "char_name"."id"  WHERE "movie_info"."info" NOT LIKE '%Portu%guese%' AND "movie_info_idx"."info_type_id" BETWEEN 100 AND 111 AND ("cast_info"."person_role_id" BETWEEN 412877.63285633945 AND 963017.6896084349 OR "cast_info"."person_role_id" <= 3140123.5839848937 OR "cast_info"."person_role_id" <= 223022.15986236685) AND "kind_type"."kind" IN ('video movie', 'movie') AND "info_type"."info" = '%LD dynamic range%' AND "keyword"."id" IS NOT NULL AND "aka_name"."id" IS NOT NULL AND "char_name"."id" IS NOT NULL;