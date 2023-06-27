FROM python:3.11-bullseye

RUN apt-get update \
    && apt install -y curl

RUN mkdir /app
WORKDIR /app

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="$PATH:/root/.local/bin"

COPY ./pyproject.toml ./poetry.lock /app/
ENV POETRY_VIRTUALENVS_CREATE=false
RUN poetry export --without-hashes --format=requirements.txt | pip install -r /dev/stdin

COPY . /app

CMD ["python", "-m", "archiver"]
