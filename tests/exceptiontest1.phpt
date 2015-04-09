--TEST--
Test custom exception class
--FILE--
<?php
$kafka = new Kafka('localhost:9092');
try {
    $kafka->isConnected('InvalidParam');
} catch (Exception $e) {
    var_dump(get_class($e));
}
?>
--EXPECT--
string(14) "KafkaException"
