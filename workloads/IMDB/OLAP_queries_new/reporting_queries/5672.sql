SELECT AVG("movie_info"."movie_id") as agg_0, AVG("movie_info"."movie_id") as agg_1 FROM "keyword" LEFT OUTER JOIN "movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" LEFT OUTER JOIN "title" ON "movie_keyword"."movie_id" = "title"."id" LEFT OUTER JOIN "cast_info" ON "title"."id" = "cast_info"."movie_id" LEFT OUTER JOIN "movie_companies" ON "title"."id" = "movie_companies"."movie_id" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id" LEFT OUTER JOIN "aka_name" ON "cast_info"."person_id" = "aka_name"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id"  WHERE "movie_companies"."note" LIKE '%(world%wide)%' AND "company_name"."country_code" LIKE '%[ca]%' AND "movie_keyword"."keyword_id" BETWEEN 21185 AND 32221 AND "title"."id" <= 1139944 AND "aka_name"."id" IS NOT NULL;