SELECT AVG("cast_info"."person_id") as agg_0 FROM "person_info" LEFT OUTER JOIN "name" ON "person_info"."person_id" = "name"."id" LEFT OUTER JOIN "aka_name" ON "name"."id" = "aka_name"."person_id" LEFT OUTER JOIN "cast_info" ON "aka_name"."id" = "cast_info"."person_id" LEFT OUTER JOIN "char_name" ON "cast_info"."person_role_id" = "char_name"."id" LEFT OUTER JOIN "title" ON "cast_info"."movie_id" = "title"."id" LEFT OUTER JOIN "kind_type" ON "title"."kind_id" = "kind_type"."id" LEFT OUTER JOIN "movie_info" ON "title"."id" = "movie_info"."movie_id"  WHERE ("aka_name"."person_id" <= 952808 OR ("aka_name"."person_id" <= 603899 AND "aka_name"."person_id" <= 911279)) AND "char_name"."surname_pcode" NOT LIKE '%M%5%' AND "name"."name" NOT LIKE '%Jo%hn%' AND "kind_type"."kind" NOT LIKE '%ser%ies%' AND "cast_info"."note" NOT LIKE '%(voice)%' AND ("aka_name"."surname_pcode" IN ('B65', 'M6', 'L', 'J52', 'B2', 'L2') OR "aka_name"."name_pcode_cf" LIKE '%A5362%') AND "movie_info"."movie_id" IS NOT NULL;