SELECT SUM("person_info"."person_id") as agg_0, AVG("cast_info"."movie_id") as agg_1 FROM "aka_name" LEFT OUTER JOIN "name" ON "aka_name"."person_id" = "name"."id" LEFT OUTER JOIN "cast_info" ON "aka_name"."id" = "cast_info"."person_id" LEFT OUTER JOIN "person_info" ON "name"."id" = "person_info"."person_id" LEFT OUTER JOIN "char_name" ON "cast_info"."person_role_id" = "char_name"."id"  WHERE "cast_info"."note" NOT LIKE '%(as%' AND "person_info"."person_id" >= 1612382 AND "name"."id" <= 1296038 AND "name"."gender" != '%f%' AND ("aka_name"."surname_pcode" IS NOT NULL OR "aka_name"."surname_pcode" LIKE '%B62%' OR "aka_name"."surname_pcode" NOT LIKE '%B62%') AND "char_name"."id" IS NOT NULL;