FROM ubuntu:20.04
WORKDIR /codeRepo
ADD . /codeRepo
RUN apt-get install -y && apt-get update && apt-get install python3 python3-pip -y
CMD pip install -r src/requirements.txt
CMD kedro run
LABEL color=red