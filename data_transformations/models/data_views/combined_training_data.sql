select
    tdr.tconst,
    tdr.primaryTitle,
    tdr.startYear,
    CASE WHEN tdr.originalTitle = '\\N' THEN NULL else tdr.originalTitle END AS originalTitle,
    CASE WHEN tdr.endYear = '\\N' THEN NULL else tdr.endYear END AS endYear,
    CASE WHEN tdr.runtimeMinutes = '\\N' THEN NULL else try_cast(tdr.runtimeMinutes as integer) END AS runtimeMinutes,
    CASE WHEN tdr.numVotes = '\\N' THEN NULL else try_cast(tdr.numVotes as integer) END AS runtimeMinutes,
    dr.director,
    wr.writer
from {{ ref('training_data_raw') }} tdr
left join {{ ref('directing_raw') }} dr on tdr.tconst = dr.movie
left join {{ ref('writing_raw') }} wr on dr.movie = wr.movie
