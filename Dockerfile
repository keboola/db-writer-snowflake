FROM php:5.6.21
MAINTAINER Miroslav Cillik <miro@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

# Install Dependencies
RUN apt-get update \
  && apt-get install unzip git unixODBC-dev libpq-dev -y

RUN docker-php-ext-install pdo_pgsql pdo_mysql
RUN pecl install xdebug \
  && docker-php-ext-enable xdebug

# snowflake odbc - https://github.com/docker-library/php/issues/103
RUN set -x \
&& cd /usr/src/php/ext/odbc \
&& phpize \
&& sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
&& ./configure --with-unixODBC=shared,/usr \
&& docker-php-ext-install odbc

## install snowflake drivers
ADD snowflake_linux_x8664_odbc.tgz /usr/bin
ADD ./driver/simba.snowflake.ini /etc/simba.snowflake.ini
ADD ./driver/odbcinst.ini /etc/odbcinst.ini
RUN mkdir -p  /usr/bin/snowflake_odbc/log

ENV SIMBAINI /etc/simba.snowflake.ini
ENV SSL_DIR /usr/bin/snowflake_odbc/SSLCertificates/nssdb
ENV LD_LIBRARY_PATH /usr/bin/snowflake_odbc/lib

# snowflake - charset settings
ENV LANG en_US.UTF-8

# install composer
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /usr/local/etc/php/conf.d/php.ini
RUN echo "date.timezone = "Europe/Prague" >> /usr/local/etc/php/conf.d/php.ini
RUN composer install --no-interaction

ENTRYPOINT php ./run.php --data=/data

