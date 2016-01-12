
cp ~/phpkafka/
sudo cp lib/librdkafka* /usr/local/lib/
sudo cp lib/centos-6.7-64.bit/php-5.3/kafka.so /usr/local/php-5.3/extensions/
sudo cp lib/centos-6.7-64.bit/php-5.6/kafka.so /usr/local/php-5.6/extensions/

sudo ldconfig

cp /usr/local/lib/
sudo ln -s librdkafka.so.1 librdkafka.so
sudo ln -s librdkafka++.so.1 librdkafka++.so

php -c kafka.ini testkafka.php

