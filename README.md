## Main dev-branch is here!

This is, currently, the main development branch for this fork. To build the extension, nothing much has changed.
However, this extension now relies on the meta API of librdkafka. Given that some systems have packaged this lib, it is important to ensure that your package is up-to-date.

If you are running a debian-based system, and have installed the `librdkafka1` or `librdkafka-dev` package, you will need to purge it, clone the `librdkafka` repo, compile and install it yourself. This extension requires librdkafka version 0.8.6 (check `RD_KAFKA_VERSION`, it should be defined as `0x00080600`).

As this is (AFAIK) the only PHP extension currently being developed, I welcome all contributions.

### Future plans

If this extension reaches something resembling an alpha-state, I'd like to start work on PHP7 support. If anyone with any experience with the new Zend API, _and_ is interested to contribute, just let me know and I'd be happy to make you a collaborator.

phpkafka
========

**Note: The library is not supported by author anymore. Please check other forks or make your own.**

PHP extension for **Apache Kafka 0.8**. It's built on top of kafka C driver ([librdkafka](https://github.com/edenhill/librdkafka/)).
It makes persistent connection to kafka broker with non-blocking calls, so it should be very fast.

IMPORTANT: Library is in heavy development and some features are not implemented yet.

Requirements:
-------------
Download and install [librdkafka](https://github.com/edenhill/librdkafka/). Run `sudo ldconfig` to update shared libraries.

Installing PHP extension:
----------
```bash
phpize
./configure --enable-kafka
make
sudo make install
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/conf.d/kafka.ini'
#For CLI mode:
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/cli/conf.d/20-kafka.ini'
```

Examples:
--------
```php
// Produce a message
$kafka = new Kafka("localhost:9092");
$kafka->produce("topic_name", "message content");
//get all the available partitions
$partitions = $kafka->getPartitionsForTopic('topic_name');
//use it to OPTIONALLY specify a partition to consume from
//if not, consuming IS slower. To set the partition:
$kafka->setPartition($partitions[0]);//set to first partition
//then consume, for example, starting with the first offset, consume 20 messages
$msg = $kafka->consume("topic_name", Kafka::OFFSET_BEGIN, 20);
var_dump($msg);//dumps array of messages
```
