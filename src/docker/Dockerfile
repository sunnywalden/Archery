FROM sunnywalden/archery-base:v1.0

WORKDIR /opt/archery

COPY . /opt/archery

#archery
RUN cd /opt \
    && source /opt/venv4archery/bin/activate \
    && cd /opt/archery \
    && pip3 install -r /opt/archery/requirements.txt \
    && cp /opt/archery/src/docker/nginx.conf /etc/nginx/ \
    && mv /opt/sqladvisor /opt/archery/src/plugins/ \
    && mv /opt/soar /opt/archery/src/plugins/ \
    && mv /opt/tmp_binlog2sql /opt/archery/src/plugins/binlog2sql

#port
EXPOSE 9123

#start service
ENTRYPOINT bash /opt/archery/src/docker/startup.sh && bash