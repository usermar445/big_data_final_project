    SELECT tconst, Year
    FROM {{ ref('clean_training_data') }}
    UNION
    SELECT tconst,Year
    FROM {{ ref('clean_validation_data') }}
    UNION
    SELECT tconst,Year
    FROM {{ ref('clean_test_data') }}
    ORDER BY Year, tconst