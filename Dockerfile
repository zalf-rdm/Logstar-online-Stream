FROM python:3.13-bookworm

RUN apt-get update && apt-get -y install git libpq-dev python3-dev python3-psycopg2
WORKDIR "/"
# INSTALL Python code
COPY . "/Logstar-online-Stream"
WORKDIR "/Logstar-online-Stream"
RUN pip install .

CMD ["/Logstar-online-Stream/docker/entrypoint.sh"]
