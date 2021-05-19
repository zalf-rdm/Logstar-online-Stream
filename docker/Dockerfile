FROM python:3.9.5-slim-buster

ARG TOKEN 

RUN apt-get update && apt-get -y install git

WORKDIR "/"
# COPY ENTRYPOINT
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# INSTALL Python code
RUN git clone https://$TOKEN@github.com/Leibniz-Zentrum-ZALF-e-V/Logstar-online-Stream.git
WORKDIR "/Logstar-online-Stream"
RUN python -m pip install -r requirements.txt

ENTRYPOINT /Logstar-online-Stream/docker/entrypoint.sh