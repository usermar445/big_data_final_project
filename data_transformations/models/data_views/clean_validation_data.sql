SELECT
        tconst,
        REGEXP_REPLACE(
                TRANSLATE(LOWER(TRIM(primaryTitle)),
                          'áàãäåæßçéèêíîïñòóôöøớúûüý',
                          'aaaaaabceeeiiinoooooouuuy'),
                '[^a-zA-Z0-9 ]',
                '',
                'g'
        ) AS primaryTitle,
        REGEXP_REPLACE(
                TRANSLATE(LOWER(TRIM(originalTitle)),
                          'áàãäåæßçéèêíîïñòóôöøớúûüý',
                          'aaaaaabceeeiiinoooooouuuy'),
                '[^a-zA-Z0-9 ]',
                '',
                'g'
        ) AS oTitle,

        CASE
           WHEN originalTitle IS NULL THEN 0
           ELSE 1
        END AS ForeignFilm,

        LENGTH(primaryTitle) - LENGTH(REPLACE(primaryTitle, ' ', '')) + 1 AS n_words,

        CASE
            WHEN startYear LIKE '%N%' THEN TRY_CAST(endYear AS INTEGER)
            ELSE TRY_CAST(startYear AS INTEGER)
        END AS Year,
        runtimeMinutes,
        numVotes
    FROM {{ ref('validation_data_raw') }}