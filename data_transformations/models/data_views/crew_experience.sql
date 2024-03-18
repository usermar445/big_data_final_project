    SELECT tconst, Year, writer, director,
    COUNT(DISTINCT tconst) OVER(PARTITION BY writer ORDER BY Year, tconst) - 1 AS writer_experience,
    COUNT(DISTINCT tconst) OVER(PARTITION BY director ORDER BY Year, tconst) - 1 AS director_experience,
    FROM {{ ref('movie_year') }} my
    LEFT JOIN {{ ref('directing_raw') }} w ON w.movie == my.tconst
    LEFT JOIN {{ ref('writing_raw') }} d ON d.movie == my.tconst
    ORDER BY Year, tconst