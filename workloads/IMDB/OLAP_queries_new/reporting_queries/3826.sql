SELECT COUNT(*) as agg_0, COUNT(*) as agg_1 FROM "company_type" LEFT OUTER JOIN "movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id"  WHERE "title"."title" NOT LIKE '%The%' AND ("company_type"."kind" IN ('distributors', 'miscellaneous companies') OR ("company_type"."kind" LIKE '%compani%es%' AND "company_type"."kind" LIKE '%companies%')) AND "movie_companies"."id" <= 496918 AND "movie_companies"."note" LIKE '%(Germa%ny)%';