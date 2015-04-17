#!/usr/bin/env bash
echo "....Update packages....."
sudo apt-get update

echo "....fetching librdkafka dependency...."
mkdir tmp_build
cd tmp_build
## clone fork, we know this version of librdkafka works
git clone https://github.com/EVODelavega/librdkafka.git
echo ".....done....."
cd librdkafka
echo "....compiling librdkafka...."
./configure && make && sudo install
echo "....done, now cleaning up...."
cd ../../
rm -Rf tmp_build
echo ".... ensure php build tools are available....."
sudo apt-get install php5-dev
echo ".... start building extension....."
phpize
./configure --enable-kafka
make
NO_INTERACTION=1 make test
