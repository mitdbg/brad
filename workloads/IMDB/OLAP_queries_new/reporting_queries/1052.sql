SELECT COUNT(*) as agg_0, MAX("title"."production_year") as agg_1 FROM "movie_companies" LEFT OUTER JOIN "company_name" ON "movie_companies"."company_id" = "company_name"."id" LEFT OUTER JOIN "company_type" ON "movie_companies"."company_type_id" = "company_type"."id" LEFT OUTER JOIN "title" ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "movie_info_idx" ON "title"."id" = "movie_info_idx"."movie_id" LEFT OUTER JOIN "info_type" ON "movie_info_idx"."info_type_id" = "info_type"."id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id"  WHERE "kind_type"."kind" NOT LIKE '%movie%' AND "company_name"."id" >= 47780 AND "movie_companies"."note" LIKE '%(%2005)%' AND "company_type"."id" IS NOT NULL AND "info_type"."id" IS NOT NULL;