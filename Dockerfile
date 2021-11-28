FROM chriskrams/odbc_17only:0.0.1

RUN apt-get update && apt-get -y install git libpq-dev python3-dev 
WORKDIR "/"
# INSTALL Python code
RUN git clone https://github.com/Leibniz-Zentrum-ZALF-e-V/Logstar-online-Stream.git
WORKDIR "/Logstar-online-Stream"
RUN python -m pip install -r requirements.txt
RUN chmod +x /Logstar-online-Stream/entrypoint.sh

CMD /Logstar-online-Stream/entrypoint.sh