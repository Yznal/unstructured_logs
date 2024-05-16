FROM python:3.9.6

WORKDIR /Drain3
COPY . .
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt
RUN python -m nltk.downloader punkt
RUN python -m nltk.downloader averaged_perceptron_tagger
RUN python -m nltk.downloader wordnet
USER root
ENTRYPOINT ["python3", "-u", "-m", "examples.drain_kafka_demo"]