FROM python:3.11-slim-bullseye

ARG PIP_EXTRA_INDEX_URL
ARG PIP_TRUSTED_HOST
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

RUN apt-get update -y
RUN apt-get install -y gdal-bin

WORKDIR /aimlsse/app
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD uvicorn satellite_data_service.main:app --host 0.0.0.0 --port 8010

EXPOSE 8010
