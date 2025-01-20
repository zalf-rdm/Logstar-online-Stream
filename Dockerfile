FROM python:3.10-buster

RUN apt-get update && apt-get -y install git libpq-dev python3-dev python-psycopg2
WORKDIR "/"
# INSTALL Python code
COPY . "/Logstar-online-Stream"
WORKDIR "/Logstar-online-Stream"
RUN pip install .

CMD /Logstar-online-Stream/docker/entrypoint.sh
