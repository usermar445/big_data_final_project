from pandera.typing import Series
import pandera as pa


class TestValidation(pa.DataFrameModel):

    tconst: Series[str] = pa.Field(str_startswith="tt", nullable=False)
    primaryTitle: Series[str] = pa.Field(nullable=False)
    originalTitle: Series[str] = pa.Field(nullable=True)
    startYear: Series[str] = pa.Field(nullable=False, coerce=True)
    endYear: Series[str] = pa.Field(nullable=True)
    runtimeMinutes: Series[str] = pa.Field(nullable=True, coerce=True)
    numVotes: Series[float] = pa.Field(nullable=True)


class TrainingValidation(TestValidation):

    label: Series[bool] = pa.Field(nullable=False)


class DirectorsValidation(pa.DataFrameModel):

    director: Series[str] = pa.Field(nullable=False)
    movie: Series[str] = pa.Field(str_startswith="tt", nullable=False)


class WritersValidation(pa.DataFrameModel):

    writer: Series[str] = pa.Field(nullable=False)
    movie: Series[str] = pa.Field(str_startswith="tt", nullable=False)
