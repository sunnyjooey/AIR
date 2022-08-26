FROM python:3.6

MAINTAINER Karthik Jallawaram "kjallawaram@air.org"

RUN apt-get update && \
        apt-get install -y \
        build-essential \
        cmake \
        git \
        wget \
        unzip \
        yasm \
        pkg-config \
        libswscale-dev \
        libtbb2 \
        libtbb-dev \
        libjpeg-dev \
        libpng-dev \
        libtiff-dev \
        libavformat-dev \
        libpq-dev \
        libgtk-3-dev \
        libboost-all-dev \
        vim \
        ffmpeg \
        python-mysqldb


WORKDIR /teacher-effectiveness

# Copy relevant files to the work directory
COPY requirements.txt /teacher-effectiveness
RUN pip install -r requirements.txt

# For back-end code
COPY models /teacher-effectiveness/models
COPY jargon-wl /teacher-effectiveness/jargon-wl
COPY *.py /teacher-effectiveness/
COPY config.ini /teacher-effectiveness/

# For flask app
COPY static /teacher-effectiveness/static
COPY templates /teacher-effectiveness/templates

# For pytest
COPY test.sh /teacher-effectiveness
COPY tests /teacher-effectiveness/tests

# SSL certificate and key
COPY cert /teacher-effectiveness/cert

ENV FLASK_ENV=development

EXPOSE 5000

CMD gunicorn --bind=0.0.0.0:5000 --workers=4 --preload --certfile=cert/cert.pem --keyfile=cert/key.pem --access-logfile=- wsgi:app
