SELECT COUNT(*) as agg_0 FROM "movie_companies" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id" LEFT OUTER JOIN "company_type" ON "movie_companies"."company_type_id" = "company_type"."id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_keyword" ON "title"."id" = "movie_keyword"."movie_id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "keyword" ON "movie_keyword"."keyword_id" = "keyword"."id" LEFT OUTER JOIN "cast_info" ON "title"."id" = "cast_info"."movie_id"  WHERE "cast_info"."note" LIKE '%(ar%chive%' AND "keyword"."id" >= 65448 AND "company_name"."country_code" IN ('[us]', '[au]', '[es]', '[gb]') AND "company_type"."id" IS NOT NULL AND "movie_info"."movie_id" IS NOT NULL AND "kind_type"."id" IS NOT NULL;