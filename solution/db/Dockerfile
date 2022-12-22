FROM postgres:14

RUN localedef -i ru_RU -c -f UTF-8 -A /usr/share/locale/locale.alias ru_RU.UTF-8
ENV LANG ru_RU.utf8

COPY ./sql/*.sql /docker-entrypoint-initdb.d/
RUN chmod +rx /docker-entrypoint-initdb.d/*.sql