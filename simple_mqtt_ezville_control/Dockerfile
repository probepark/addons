FROM python:3.12-slim

ENV LANG C.UTF-8
ENV TZ=Asia/Seoul
ENV PYTHONUNBUFFERED=1

# Install requirements for add-on
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir "paho-mqtt>=2.0,<3.0"

# Copy data for add-on
COPY run.sh /
COPY ezville.py /

WORKDIR /share

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
