SELECT MIN("movie_companies"."company_type_id") as agg_0, MIN("kind_type"."id") as agg_1 FROM "kind_type" LEFT OUTER JOIN "title" ON "kind_type"."id" = "title"."kind_id" LEFT OUTER JOIN "movie_companies" ON "title"."id" = "movie_companies"."movie_id" LEFT OUTER JOIN "company_type" ON "movie_companies"."company_type_id" = "company_type"."id"  WHERE "company_type"."id" = 1 AND "movie_companies"."note" LIKE '%(200%5)%' AND "kind_type"."id" <= 2;