SELECT MAX("title"."id") as agg_0, AVG("title"."id") as agg_1 FROM "movie_keyword" LEFT OUTER JOIN "keyword" ON "movie_keyword"."keyword_id" = "keyword"."id" LEFT OUTER JOIN "title" ON "movie_keyword"."movie_id" = "title"."id"  WHERE "movie_keyword"."id" >= 2871600 AND "keyword"."phonetic_code" LIKE '%R1%652%' AND ("title"."title" LIKE '%P%art%' OR "title"."title" NOT LIKE '%T%he%' OR "title"."title" NOT LIKE '%i%n%');