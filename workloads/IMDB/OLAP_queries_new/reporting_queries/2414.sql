SELECT COUNT(*) as agg_0 FROM "person_info" LEFT OUTER JOIN "name" ON "person_info"."person_id" = "name"."id" LEFT OUTER JOIN "aka_name" ON "name"."id" = "aka_name"."person_id"  WHERE ("name"."id" BETWEEN 1428706 AND 2444396 OR "name"."id" <= 1042558 OR "name"."id" BETWEEN 1431021 AND 4107610) AND ("aka_name"."name_pcode_cf" NOT LIKE '%A5362%' OR ("aka_name"."name_pcode_cf" LIKE '%A5362%' AND "aka_name"."name_pcode_cf" NOT LIKE '%A5362%')) AND "aka_name"."surname_pcode" LIKE '%B65%' AND "name"."name" NOT LIKE '%Michae%l%' AND "person_info"."person_id" BETWEEN 297173 AND 2794592;