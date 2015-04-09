#This fork is still being actively developed

Given that the original repo is no longer supported by the author, I've decided to keep working at this PHP-Kafka extension instead.
The branch where most of the work is being done is the `consume-with-meta` branch.

Changes that have happened thusfar:

* Timeout when disconnecting is reduced (significantly)
* Connections can be closed as you go
* The librdkafka meta API is used
* New methods added (`Kafka::getTopics`, `Kafka::getPartitionsFor($topic)` most notable additions)
* `Kafka::set_partition` is deprecated, in favour of the more PSR-compliant `Kafka::setPartition` method
* A PHP stub was added for IDE code-completion
* Argument checks were added, and exceptions are thrown in some places
* Class constants for an easier API (`Kafka::OFFSET_*`)
* The extension logged everything in `/var/etc/syslog`, this is still the default behaviour (as this extension is under development), but can be turned off (`Kafka::setLogLevel(Kafka::LOG_OFF)`)
* Exceptions (`KafkaException`) in case of errors (still work in progress, though)
* Thread-safe Kafka connections

Changes that are on the _TODO_ list include:

* Separating kafka meta information out into a separate class (`KafkaTopic`  and `KafkaMessage` classes)
* Support for multiple kafka connections
* Allow PHP to determine what the timeouts should be (mainly when disconnecting, or producing messages)
* Add custom exceptions
* Overall API improvements
* Performance!
* CI (travis)
* Adding tests to the build
* PHP7 support

All help is welcome, of course...


PHP extension for **Apache Kafka 0.8**. It's built on top of kafka C driver ([librdkafka](https://github.com/edenhill/librdkafka/)).
This extension requires the version 0.8.6 (ubuntu's librdkafka packages won't do it, they do not implement the meta API yet).

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
