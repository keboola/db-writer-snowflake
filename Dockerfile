FROM php:7.1
MAINTAINER Miroslav Cillik <miro@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

# Install Dependencies
RUN apt-get update \
  && apt-get install unzip git unixODBC-dev libpq-dev -y


# snowflake odbc - https://github.com/docker-library/php/issues/103
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

## install snowflake drivers
ADD snowflake-odbc.deb /tmp/snowflake-odbc.deb
ADD ./driver/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini
RUN apt-get install -y libnss3-tools && dpkg -i /tmp/snowflake-odbc.deb

# snowflake - charset settings
ENV LANG en_US.UTF-8

# install composer
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /usr/local/etc/php/conf.d/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/conf.d/php.ini
RUN composer install --no-interaction

CMD php ./run.php --data=/data

