FROM python:3.10-buster

RUN apt-get update && apt-get -y install git libpq-dev python3-dev 
WORKDIR "/"
# INSTALL Python code
COPY . "/Logstar-online-Stream"
WORKDIR "/Logstar-online-Stream"
RUN python -m pip install -r requirements.txt

CMD /Logstar-online-Stream/entrypoint.sh