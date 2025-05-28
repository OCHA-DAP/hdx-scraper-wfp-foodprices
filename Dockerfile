FROM public.ecr.aws/unocha/python:3.13-stable

LABEL org.label-schema.hdx.scraper.step.1=true \
      org.label-schema.hdx.scraper.step.2=true

WORKDIR /srv

COPY . .

RUN --mount=source=.git,target=.git,type=bind \
    apk add --no-cache --upgrade --virtual .build-deps \
        git && \
    pip install --no-cache-dir . && \
    apk del .build-deps && \
    rm -rf /var/lib/apk/*

CMD "python3 run.py"
