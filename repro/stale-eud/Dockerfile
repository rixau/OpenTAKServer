# Runtime image for the stale EUD session reproduction.
# Dependencies come from the PyPI release; the OTS code that actually runs is
# whatever checkout compose mounts at /code (PYTHONPATH wins over site-packages),
# so the same image runs both the unmodified and the fixed code.
FROM python:3.13
ARG OTS_VERSION=1.7.14
RUN pip install --no-cache-dir "opentakserver==${OTS_VERSION}"
ENV PYTHONPATH=/code PYTHONUNBUFFERED=1
WORKDIR /app
