SELECT COUNT(*) as agg_0 FROM "aka_name" LEFT OUTER JOIN "name" ON "aka_name"."person_id" = "name"."id" LEFT OUTER JOIN "person_info" ON "name"."id" = "person_info"."person_id" LEFT OUTER JOIN "cast_info" ON "aka_name"."id" = "cast_info"."person_id" LEFT OUTER JOIN "title" ON "cast_info"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_keyword" ON "title"."id" = "movie_keyword"."movie_id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id"  WHERE "cast_info"."note" NOT LIKE '%Charlie%' AND "cast_info"."id" <= 531757 AND "person_info"."info" LIKE '%de%' AND "cast_info"."nr_order" IS NOT NULL AND "movie_keyword"."movie_id" IS NOT NULL AND "kind_type"."id" IS NOT NULL;