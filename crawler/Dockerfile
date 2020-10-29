FROM python:3.7

RUN mkdir /app
ENV APP_ROOT /app
WORKDIR $APP_ROOT

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && ./aws/install
COPY requirements.txt $APP_ROOT

RUN pip install -r requirements.txt

COPY . .
