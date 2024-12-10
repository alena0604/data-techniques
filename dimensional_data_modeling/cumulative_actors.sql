WITH last_year AS (
    SELECT * FROM actors_table
    WHERE current_year = {prev_year}
),
this_year AS (
    SELECT * FROM actor_films
    WHERE year = {current_year}
)

SELECT
        COALESCE(ly.actorid, ty.actorid) as actorid,
        COALESCE(ly.filmid, ty.filmid) AS filmid,
        {current_year} AS year
        CASE
             WHEN ty.film IS NOT NULL THEN
                 (CASE WHEN ty.rating > 8 THEN 'star'
                    WHEN ty.rating > 7 THEN 'good'
                    WHEN ty.rating > 6 THEN 'average'
                    ELSE 'bad' END)::quality_class_type
             ELSE ly.quality_class
         END as quality_class,
        COALESCE(ly.films,
            ARRAY[]::film_type[]
            ) || CASE WHEN ty.film IS NOT NULL THEN
                ARRAY[ROW(
                ty.film,
                ty.votes,
                ty.rating,
                ty.filmid)::film_type]
                ELSE ARRAY[]::film_type[] END
            as films,
         ty.film IS NOT NULL as is_active

    FROM last_year ly
    FULL OUTER JOIN this_year ty
    ON ly.actorid = ty.actorid
    AND ly.filmid = ty.filmid;