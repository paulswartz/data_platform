FROM python:3.7.12-slim-buster AS builder

RUN useradd cubic_dmap -m
USER cubic_dmap

WORKDIR /home/cubic_dmap

ENV PATH=/home/cubic_dmap/.local/bin:$PATH

RUN pip install poetry==1.1.13
RUN poetry config virtualenvs.in-project true

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry install --no-dev

COPY --chown=cubic_dmap cubic_dmap cubic_dmap
RUN poetry run python -m compileall cubic_dmap

COPY --chown=cubic_dmap sample_api.py sample_api.py

ENTRYPOINT ["poetry", "run"]
CMD ["python", "sample_api.py"]
