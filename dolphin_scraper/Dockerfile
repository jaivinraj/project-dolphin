# base image
FROM python:3.8

# ubuntu installing - python, pip, graphviz, nano, libpq (for psycopg2)
RUN apt-get update &&\
    apt-get install graphviz -y

# making directory of app
WORKDIR /app

# copy over requirements
COPY requirements.txt ./requirements.txt

# install pip then packages
RUN pip install -r requirements.txt

# exposing default port for dash
EXPOSE 8050
EXPOSE 8920

# cmd to launch app when container is run
# CMD ["python", "app.py"]