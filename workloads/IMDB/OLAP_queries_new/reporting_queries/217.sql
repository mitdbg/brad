SELECT MAX("person_info"."id") as agg_0, SUM("movie_info_idx"."info_type_id") as agg_1 FROM "aka_name" LEFT OUTER JOIN "cast_info" ON "aka_name"."id" = "cast_info"."person_id" LEFT OUTER JOIN "title" ON "cast_info"."movie_id" = "title"."id" LEFT OUTER JOIN "name" ON "aka_name"."person_id" = "name"."id" LEFT OUTER JOIN "person_info" ON "name"."id" = "person_info"."person_id" LEFT OUTER JOIN "movie_info_idx" ON "title"."id" = "movie_info_idx"."movie_id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "movie_companies" ON "title"."id" = "movie_companies"."movie_id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id"  WHERE "cast_info"."movie_id" <= 2463649 AND "company_name"."country_code" IN ('[de]', '[jp]', '[es]', '[gb]', '[it]', '[au]') AND "person_info"."info" LIKE '%after%' AND ("cast_info"."note" LIKE '%Charlie%' OR ("cast_info"."note" NOT LIKE '%(a%s%' AND "cast_info"."note" LIKE '%footage)%')) AND "movie_info"."movie_id" IS NOT NULL AND "kind_type"."id" IS NOT NULL;