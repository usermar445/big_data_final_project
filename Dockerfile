FROM python:3.12

WORKDIR /big-data

RUN pip install poetry
RUN python -m venv /venv

COPY poetry.lock pyproject.toml /big-data/

RUN poetry install --no-root

COPY . /big-data

EXPOSE 8080

RUN echo $DIRECTING_FILE_LOC

CMD ["make", "docker-dagster"]
