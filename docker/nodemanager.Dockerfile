FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

# Debian 9 (stretch) is EOL; point apt at the archive mirror so we can
# still install packages, and skip the expired Release-file date check.
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
