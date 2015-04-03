 #!/usr/bin/env bash
CONF_FILE="configure"
if [ -e $CONF_FILE ] ; then
   ./configure clean
fi
make clean
phpize
./configure --enable-kafka
make
sudo make install

