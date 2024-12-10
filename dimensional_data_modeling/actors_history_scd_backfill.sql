INSERT INTO actors_scd
WITH streak_started AS (
    SELECT actorid,
           current_year,
           quality_class,
		   is_active,
           LAG(quality_class, 1) OVER
               (PARTITION BY actorid ORDER BY current_year) <> quality_class
               OR LAG(quality_class, 1) OVER
               (PARTITION BY actorid ORDER BY current_year) IS NULL
               AS did_change
    FROM actors_table
),
     streak_identified AS (
         SELECT
                actorid,
                quality_class,
				is_active,
                current_year,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actorid ORDER BY current_year) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actorid,
            quality_class,
			is_active,
            streak_identifier,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date,
			2021 as current_year
         FROM streak_identified
         GROUP BY 1,2,3,4
     )

     SELECT actorid, quality_class, is_active, start_date, end_date, current_year
     FROM aggregated