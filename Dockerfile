FROM apache/spark:3.5.3

USER root
WORKDIR /opt/project
COPY . /opt/project
RUN mkdir -p src && python3 -m pip install --no-cache-dir .
ENV PYTHONPATH=/opt/project/src
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "-m", "imdb_project.cli"]
