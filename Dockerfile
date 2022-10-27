FROM python:3.7

WORKDIR /app

VOLUME ["/source", "/dest"]

ENV LOCALE=zh_CN.utf8
ENV DELETE_DUPLICATE=True
ENV FORMAT="%Y/%m/%Y%m%d_%H%M%S%%-uc.%%e"

COPY /app .
CMD [ "python", "-u", "/app/medio.py" ]