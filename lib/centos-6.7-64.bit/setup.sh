
cd ~/phpkafka/lib/centos-6.7-64.bit/
sudo cp librdkafka* /usr/local/lib/
sudo cp centos-6.7-64.bit/php-5.3/kafka.so /usr/local/php-5.3/extensions/
sudo cp centos-6.7-64.bit/php-5.6/kafka.so /usr/local/php-5.6/extensions/

cd /usr/local/lib/
sudo ln -s librdkafka.so.1 librdkafka.so
sudo ln -s librdkafka++.so.1 librdkafka++.so

ls -ls librdkafka*
ls -ls /usr/local/php-5.3/extensions/kafka.so
ls -ls /usr/local/php-5.6/extensions/kafka.so

sudo ldconfig

cd ~/phpkafka/
/usr/local/php-5.3/bin/php -c lib/kafka.ini lib/testkafka.php
/usr/local/php-5.6/bin/php -c lib/kafka.ini lib/testkafka.php
