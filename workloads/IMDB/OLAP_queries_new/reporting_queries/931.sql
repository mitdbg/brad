SELECT MIN("title"."season_nr") as agg_0, SUM("title"."kind_id") as agg_1 FROM "title" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "movie_info_idx" ON "title"."id" = "movie_info_idx"."movie_id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id"  WHERE ("title"."title" LIKE '%P%art%' OR ("title"."imdb_index" = '%XII%' AND "title"."title" NOT LIKE '%the%')) AND "kind_type"."id" BETWEEN 2 AND 5 AND ("movie_info_idx"."info" NOT LIKE '%7.6%' OR "movie_info_idx"."id" >= 955583) AND "kind_type"."kind" NOT LIKE '%s%eries%' AND ("movie_info"."info" LIKE '%Port%uguese%' OR ("movie_info"."info_type_id" BETWEEN 1 AND 3 AND "movie_info"."info" NOT LIKE '%Spanish%'));