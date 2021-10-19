SELECT artist_lastfm FROM artists
WHERE scrobbles_lastfm IN (
                            SELECT max(scrobbles_lastfm) as scrobbles_lastfm
                            FROM artists
                            )