#!/usr/bin/env bash
echo "....fetching librdkafka dependency...."
mkdir tmp_build
cd tmp_build
## clone fork, we know this version of librdkafka works
git clone https://github.com/EVODelavega/librdkafka.git
echo ".....done....."
cd librdkafka
echo "....compiling librdkafka...."
./configure && make
sudo make install
echo "....done...."
cd ../../
echo ".... ensure librdkafka is available....."
sudo ldconfig
rm -Rf tmp_build
echo ".... start building extension....."
phpize
./configure --enable-kafka
make
NO_INTERACTION=1 make test
#exit with make test exit code
exit $?
