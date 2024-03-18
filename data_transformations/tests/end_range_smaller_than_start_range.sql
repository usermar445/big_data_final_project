select *
from {{ ref('training_data_raw') }} as tdr
where tdr.startYear > tdr.endYear
and tdr.endYear is not null
and tdr.startYear not LIKE '%N%'