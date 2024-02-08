FROM public.ecr.aws/datamindedacademy/capstone:v3.4.1-hadoop-3.3.6-v1

WORKDIR /app

ENV AWS_ACCESS_KEY_ID=''
ENV AWS_SECRET_ACCESS_KEY=''
ENV AWS_DEFAULT_REGION='eu-west-1'

COPY requirements.txt /app

USER root

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY pipeline /app/pipeline

ENTRYPOINT ["python3"]
CMD ["pipeline/pipeline.py"]