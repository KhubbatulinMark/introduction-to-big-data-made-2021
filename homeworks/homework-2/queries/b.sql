SELECT tags_top.tag
FROM (
        SELECT trim(tags) AS tag, count(*) AS cnt
        FROM artists LATERAL VIEW explode(tags_lastfm) t AS tags
        WHERE trim(tags) != ''
        GROUP BY tags
        ORDER BY cnt DESC
        LIMIT 1
    ) tags_top
