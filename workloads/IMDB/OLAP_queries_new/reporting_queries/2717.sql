SELECT COUNT(*) as agg_0, COUNT(*) as agg_1 FROM "name" LEFT OUTER JOIN "person_info" ON "name"."id" = "person_info"."person_id" LEFT OUTER JOIN "aka_name" ON "name"."id" = "aka_name"."person_id" LEFT OUTER JOIN "cast_info" ON "aka_name"."id" = "cast_info"."person_id"  WHERE "cast_info"."note" NOT LIKE '%footage)%' AND "aka_name"."surname_pcode" LIKE '%B%65%' AND "aka_name"."person_id" >= 1489124 AND "person_info"."person_id" IS NOT NULL;