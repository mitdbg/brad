SELECT SUM("movie_companies"."id") as agg_0, SUM("title"."episode_nr") as agg_1 FROM "keyword" LEFT OUTER JOIN "movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" LEFT OUTER JOIN "title" ON "movie_keyword"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "movie_companies" ON "title"."id" = "movie_companies"."movie_id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "movie_info_idx" ON "title"."id" = "movie_info_idx"."movie_id"  WHERE "kind_type"."kind" LIKE '%tv%' AND "title"."imdb_index" != '%XXIV%' AND ("title"."production_year" BETWEEN 1992.6771388310372 AND 2010.2113903460665 OR "title"."production_year" BETWEEN 1992.4152694804477 AND 2013.3898028583392) AND "movie_info"."movie_id" IS NOT NULL AND "movie_info_idx"."movie_id" IS NOT NULL;