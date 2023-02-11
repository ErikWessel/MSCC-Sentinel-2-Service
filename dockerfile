FROM python:3.11-slim-bullseye

RUN apt-get update -y
RUN apt-get install -y gdal-bin

WORKDIR /aimlsse/app
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --trusted-host host.docker.internal --extra-index-url http://host.docker.internal:8060 -r requirements.txt

COPY . .

CMD uvicorn satellite_data_service.main:app --host 0.0.0.0 --port 8010

EXPOSE 8010