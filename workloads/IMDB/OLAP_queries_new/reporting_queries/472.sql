SELECT MIN("title"."episode_nr") as agg_0 FROM "movie_companies" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "company_type" ON "movie_companies"."company_type_id" = "company_type"."id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id"  WHERE "company_name"."country_code" LIKE '%[%es]%' AND "company_type"."kind" IN ('distributors', 'miscellaneous companies', 'special effects companies') AND "movie_companies"."id" >= 994627 AND "kind_type"."id" IS NOT NULL;