SELECT AVG("title"."episode_nr") as agg_0, SUM("title"."episode_of_id") as agg_1 FROM "movie_info_idx" LEFT OUTER JOIN "title" ON "movie_info_idx"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_companies" ON "title"."id" = "movie_companies"."movie_id"  WHERE "title"."title" NOT LIKE '%The%' AND "movie_companies"."note" NOT LIKE '%(Hungary)%' AND ("movie_info_idx"."info" LIKE '%7.5%' OR ("movie_info_idx"."info" LIKE '%6%.8%' AND "movie_info_idx"."info" LIKE '%7.%1%'));