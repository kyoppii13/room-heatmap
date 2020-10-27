FROM python:3.7

RUN mkdir /app
ENV APP_ROOT /app
WORKDIR $APP_ROOT

COPY requirements.txt $APP_ROOT

RUN pip install -r requirements.txt

COPY . ${APP_ROOT}
