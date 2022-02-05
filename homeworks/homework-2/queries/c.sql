SELECT DISTINCT top_artist_with_tag.artist_lastfm
FROM (
    SELECT parsed_text.artist_lastfm, parsed_text.tag, max(parsed_text.scrobbles_lastfm) as scrobbles_lastfm
    FROM (
            SELECT trim(tags) AS tag, artist_lastfm, scrobbles_lastfm
            FROM artists LATERAL VIEW explode(tags_lastfm) t AS tags
            WHERE trim(tags) != '') parsed_text,
            (
            SELECT tags_top.tag
            FROM (
                    SELECT trim(tags) AS tag, count(*) AS cnt
                    FROM artists LATERAL VIEW explode(tags_lastfm) t AS tags
                    WHERE trim(tags) != ''
                    GROUP BY tags
                    ORDER BY cnt DESC
                    LIMIT 10
                ) tags_top
            ) tags_top
    WHERE parsed_text.tag = tags_top.tag
    GROUP BY parsed_text.artist_lastfm, parsed_text.tag
) top_artist_with_tag
ORDER BY scrobbles_lastfm DESC
LIMIT 10