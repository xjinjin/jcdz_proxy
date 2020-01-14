FROM python:3.7
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN mkdir /code
COPY . /code
EXPOSE 8080
WORKDIR /code
CMD mitmdump -s /code/proxy/proxy_v1.py


