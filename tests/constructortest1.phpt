--TEST--
Test constructor arguments (config array)
--FILE--
<?php
try {
    $kafka = new Kafka('localhost:9092', [Kafka::LOGLEVEL => 123]);
} catch (KafkaException $e) {
    var_dump($e->getMessage());
}
?>
--EXPECT--
string(68) "Invalid value for Kafka::LOGLEVEL option, use Kafka::LOG_* constants"
