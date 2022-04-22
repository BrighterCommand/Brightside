FROM python:3.9
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
ADD . /code
WORKDIR /code

RUN pip install pipenv && pipenv install -e . && pipenv install --dev

ENTRYPOINT ["top", "-b"]
CMD ["-c"]

