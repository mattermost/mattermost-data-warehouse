FROM python:3.7

# Copy in required files
COPY . ./

RUN apt-get update && apt-get install -y vim libpq-dev gcc

# Install Python Requirements
RUN pip install -U pip
RUN pip install -r requirements.txt

CMD echo "hello"
