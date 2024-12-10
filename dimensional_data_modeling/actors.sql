-- Create a composite type for films
CREATE TYPE film AS (
    film TEXT,          -- The name of the film
    votes INTEGER,      -- The number of votes the film received
    rating REAL,        -- The rating of the film
    filmid TEXT         -- A unique identifier for each film
);

-- Create an ENUM type for quality_class
CREATE TYPE quality_class AS ENUM (
    'bad',              -- Rating ≤ 6
    'average',          -- Rating > 6 and ≤ 7
    'good',             -- Rating > 7 and ≤ 8
    'star'              -- Rating > 8
);

-- Create the actors table
CREATE TABLE actors (
    actorid TEXT NOT NULL,                    -- Unique identifier for the actor
    filmid TEXT NOT NULL,                     -- Unique identifier for the film
    current_year INTEGER NOT NULL,            -- The year being tracked
    quality_class quality_class,             -- Actor's performance quality
    films film[],                             -- Array of films (using the film type)
    is_active BOOLEAN,                        -- Whether the actor is currently active
    PRIMARY KEY (actorid, filmid, current_year) -- Composite primary key
);
