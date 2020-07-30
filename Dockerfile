FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y libpq-dev python3-dev python3-pip

WORKDIR /usr/src/app

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD [ "python", "write_to_file.py" ]
