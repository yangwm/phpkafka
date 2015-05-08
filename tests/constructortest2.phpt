--TEST--
Test constructor arguments (config array)
--FILE--
<?php
try {
    $kafka = new Kafka('localhost:9092', array(Kafka::PRODUCE_BATCH_SIZE => array()));
} catch (KafkaException $e) {
    var_dump($e->getMessage());
}
?>
--EXPECT--
string(70) "Invalid argument for Kafka::PRODUCE_BATCH_SIZE, expected numeric value"
