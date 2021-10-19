CREATE TABLE IF NOT EXISTS artists (
    mbid STRING,
    artist_mb STRING,
    artist_lastfm STRING,
    country_mb STRING,
    country_lastfm STRING,
    tags_mb ARRAY < STRING >,
    tags_lastfm ARRAY < STRING >,
    listeners_lastfm INT,
    scrobbles_lastfm INT,
    ambiguous_artist BOOLEAN
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ';' LINES TERMINATED BY '\n' TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/data/artists.csv' OVERWRITE INTO TABLE artists;