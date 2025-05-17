CREATE EXTENSION IF NOT EXISTS citext; 
CREATE EXTENSION IF NOT EXISTS pg_trgm;  
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS movie_spoken_languages CASCADE;
DROP TABLE IF EXISTS spoken_languages        CASCADE;
DROP TABLE IF EXISTS movie_prod_companies    CASCADE;
DROP TABLE IF EXISTS production_companies    CASCADE;
DROP TABLE IF EXISTS movie_prod_countries    CASCADE;
DROP TABLE IF EXISTS production_countries    CASCADE;
DROP TABLE IF EXISTS collections             CASCADE;
DROP TABLE IF EXISTS movie_genres            CASCADE;
DROP TABLE IF EXISTS genres                  CASCADE;
DROP TABLE IF EXISTS movies                  CASCADE;


CREATE TABLE collections (
    id            SERIAL PRIMARY KEY,
    tmdb_id       INTEGER UNIQUE NOT NULL,
    name          TEXT    NOT NULL,
    poster_path   TEXT,
    backdrop_path TEXT
);

CREATE TABLE genres (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE production_companies (
    id            SERIAL PRIMARY KEY,
    tmdb_id       INTEGER UNIQUE NOT NULL,
    name          TEXT NOT NULL,
    logo_path     TEXT,
    origin_country CHAR(2)
);

CREATE TABLE production_countries (
    iso_3166_1 CHAR(2) PRIMARY KEY,
    name       TEXT NOT NULL
);

CREATE TABLE spoken_languages (
    iso_639_1   CHAR(2) PRIMARY KEY,
    english_name TEXT NOT NULL
);


CREATE TABLE movies (
    id               SERIAL PRIMARY KEY,
    tmdb_id          INTEGER UNIQUE NOT NULL,
    title            TEXT NOT NULL,
    original_title   TEXT,
    original_language CHAR(2),
    adult            BOOLEAN NOT NULL DEFAULT FALSE,
    status           TEXT, 
    tagline          TEXT,
    overview         TEXT,
    release_date     DATE,
    runtime          INT,
    budget           BIGINT,
    revenue          BIGINT,
    popularity       NUMERIC(12,4),
    vote_count       INT,
    vote_average     NUMERIC(4,2),
    poster_path      TEXT,
    backdrop_path    TEXT,
    homepage         TEXT,
    imdb_id          TEXT,
    external_ids     JSONB,
    crew             JSONB, 
    origin_country   CHAR(2)[],
    collection_id    INT REFERENCES collections(id) ON DELETE SET NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE movie_genres (
    movie_id  INT REFERENCES movies(id) ON DELETE CASCADE,
    genre_id  INT REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE movie_prod_companies (
    movie_id     INT REFERENCES movies(id)             ON DELETE CASCADE,
    company_id   INT REFERENCES production_companies(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE movie_prod_countries (
    movie_id        INT REFERENCES movies(id)            ON DELETE CASCADE,
    iso_3166_1      CHAR(2) REFERENCES production_countries(iso_3166_1)
                                                 ON DELETE CASCADE,
    PRIMARY KEY (movie_id, iso_3166_1)
);

CREATE TABLE movie_spoken_languages (
    movie_id     INT REFERENCES movies(id)               ON DELETE CASCADE,
    iso_639_1    CHAR(2) REFERENCES spoken_languages(iso_639_1)
                                                 ON DELETE CASCADE,
    PRIMARY KEY (movie_id, iso_639_1)
);


CREATE INDEX idx_movies_release_date      ON movies(release_date);
CREATE INDEX idx_movies_popularity        ON movies(popularity DESC);
CREATE INDEX idx_movies_vote_average      ON movies(vote_average DESC);
CREATE INDEX idx_movies_crew_gin          ON movies USING GIN (crew);
CREATE INDEX idx_movies_external_ids_gin  ON movies USING GIN (external_ids);
CREATE INDEX idx_movies_origin_country_gin ON movies USING GIN (origin_country);

CREATE INDEX idx_genres_name              ON genres(name);

CREATE INDEX idx_prod_companies_name_trgm ON production_companies
    USING gin (name gin_trgm_ops);
