SELECT MIN("title"."id") as agg_0, COUNT(*) as agg_1 FROM "company_type" LEFT OUTER JOIN "movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id"  WHERE "movie_companies"."company_id" BETWEEN 25340 AND 27145 AND "movie_companies"."company_type_id" >= 1 AND ("company_type"."kind" NOT LIKE '%companies%' OR "company_type"."kind" LIKE '%c%ompanies%' OR "company_type"."kind" NOT LIKE '%compa%nies%') AND "company_type"."kind" IN ('distributors') AND "movie_companies"."note" NOT LIKE '%(%2011)%' AND "company_name"."id" IS NOT NULL AND "movie_info"."movie_id" IS NOT NULL;