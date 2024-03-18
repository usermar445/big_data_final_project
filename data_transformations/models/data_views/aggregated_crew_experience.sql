    SELECT
      tconst,
      COUNT(DISTINCT writer) AS n_writers,
      AVG(DISTINCT e.writer_experience) AS avgexp_writers,
      SUM(DISTINCT e.writer_experience) AS totexp_writers,
      COUNT(DISTINCT director) AS n_directors,
      AVG(DISTINCT e.director_experience) AS avgexp_directors,
      SUM(DISTINCT e.director_experience) AS totexp_directors,
    FROM {{ ref('crew_experience') }} e
    GROUP BY tconst