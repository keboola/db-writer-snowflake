FROM php:8.2-cli-buster

ENV DEBIAN_FRONTEND noninteractive
ARG SNOWFLAKE_ODBC_VERSION=3.4.1
ARG SNOWFLAKE_GPG_KEY=630D9F3CAB551AF3
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ENV LANG en_US.UTF-8
ENV LC_ALL=C.UTF-8

WORKDIR /code

COPY docker/composer-install.sh /tmp/composer-install.sh
COPY ./docker/php/php.ini /usr/local/etc/php/php.ini

# download snowflake odbc driver
ADD ./docker/snowflake/snowflake-policy.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY/generic.pol
ADD ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini
ADD https://sfc-repo.azure.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb /tmp/snowflake-odbc.deb

# Install Dependencies
RUN  apt-get update && apt-get install -y --no-install-recommends \
        unzip \
        git \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        debsig-verify \
        dirmngr \
        gpg-agent \
        locales \
        libicu-dev \
        ssh \
    && rm -r /var/lib/apt/lists/* \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

# Verify and install snowflake odbc driver
RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY \
    && if ! gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_GPG_KEY; then \
        gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_GPG_KEY;  \
    fi \
    && gpg --export $SNOWFLAKE_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY/debsig.gpg \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb \
    && rm /tmp/snowflake-odbc.deb

# Install PHP odbc extension
# https://github.com/docker-library/php/issues/103
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
COPY patches /code/patches

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD php /code/src/run.php