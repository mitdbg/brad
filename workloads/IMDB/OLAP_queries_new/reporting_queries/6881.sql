SELECT SUM("title"."episode_nr") as agg_0, SUM("title"."episode_nr") as agg_1 FROM "movie_info_idx" LEFT OUTER JOIN "info_type" ON "movie_info_idx"."info_type_id" = "info_type"."id" LEFT OUTER JOIN "title" ON "movie_info_idx"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"  WHERE "info_type"."info" NOT LIKE '%rel%ease%' AND "title"."title" LIKE '%Par%t%' AND "movie_info_idx"."info" NOT LIKE '%10%' AND "info_type"."info" NOT LIKE '%produc%tion%' AND "movie_keyword"."movie_id" IS NOT NULL;