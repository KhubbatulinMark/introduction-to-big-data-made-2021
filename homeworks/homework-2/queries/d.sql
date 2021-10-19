WITH german as (
                SELECT *
                FROM artists
                WHERE country_mb = 'Germany'
                ORDER BY scrobbles_lastfm DESC
                LIMIT 1
                ),
    tags_mb as
                (                            SELECT artist_mb, t.tags_mb
                            FROM german LATERAL VIEW explode(tags_mb) t AS tags_mb)
                , tags_lastfm as (
                                SELECT artist_mb, t.tags_lastfm
                                FROM german LATERAL VIEW explode(tags_lastfm) t AS tags_lastfm
                                )
SELECT tags_lastfm.artist_mb as artist_mb, tags_lastfm.tags_lastfm as tags, 'last_fm' as owner
FROM tags_lastfm
WHERE NOT EXISTS (
                SELECT NULL
                FROM tags_mb
                WHERE upper(tags_mb.tags_mb) = upper(tags_lastfm.tags_lastfm)
                )
UNION
SELECT tags_mb.artist_mb, tags_mb.tags_mb, 'mb' as owner
FROM tags_mb
WHERE NOT EXISTS (
                SELECT NULL
                FROM tags_lastfm
                WHERE upper(tags_mb.tags_mb) = upper(tags_lastfm.tags_lastfm)
                )
UNION
SELECT tags_mb.artist_mb, tags_mb.tags_mb, 'both' as owner
FROM tags_mb
WHERE EXISTS (
                SELECT NULL
                FROM tags_lastfm
                WHERE upper(tags_mb.tags_mb) = upper(tags_lastfm.tags_lastfm)
                )