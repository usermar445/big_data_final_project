SELECT 
      td.tconst, 
      td.Year AS Year,
      td.runtimeMinutes,
      td.n_words, 
      td.numVotes,
      e.n_writers,
      e.avgexp_writers,
      e.totexp_writers,
      e.n_directors,
      e.avgexp_directors,
      e.totexp_directors,
      td.label
      
    FROM {{ ref('clean_training_data') }} td
    LEFT JOIN {{ ref('aggregated_crew_experience') }} e ON e.tconst == td.tconst