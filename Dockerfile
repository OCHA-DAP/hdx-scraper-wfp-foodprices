FROM public.ecr.aws/unocha/python:3.12-stable

WORKDIR /srv

COPY . .

RUN apk add --no-cache --upgrade --virtual .build-deps \
    git && \
    pip --no-cache-dir install --upgrade -r requirements.txt && \
    apk del .build-deps

CMD ["python3", "run.py"]
