SELECT MIN("title"."episode_of_id") as agg_0, MAX("movie_companies"."id") as agg_1 FROM "movie_companies" LEFT OUTER JOIN "company_type" ON "movie_companies"."company_type_id" = "company_type"."id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id" LEFT OUTER JOIN "movie_keyword" ON "title"."id" = "movie_keyword"."movie_id" LEFT OUTER JOIN "movie_info_idx" ON "title"."id" = "movie_info_idx"."movie_id"  WHERE ("company_type"."kind" IN ('production companies', 'distributors') OR "company_type"."id" != 1 OR "company_type"."kind" != '%distributors%') AND "movie_companies"."note" LIKE '%(2%003)%' AND ("kind_type"."id" >= 6 OR "kind_type"."kind" NOT LIKE '%video%') AND "company_name"."country_code" LIKE '%[j%p]%' AND "movie_companies"."company_type_id" <= 2 AND "movie_info"."movie_id" IS NOT NULL AND "movie_keyword"."movie_id" IS NOT NULL AND "movie_info_idx"."movie_id" IS NOT NULL;