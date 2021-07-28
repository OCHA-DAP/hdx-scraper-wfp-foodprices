FROM unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

COPY . .

RUN apk add --no-cache --upgrade --virtual .build-deps \
    build-base \
    python3-dev && \
    pip --no-cache-dir install -r docker-requirements.txt && \
    apk del .build-deps && \
    rm -rf ./target ~/.cargo /var/lib/apk/*

CMD ["python3", "run.py"]
