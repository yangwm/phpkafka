--TEST--
Basic test - Kafka::isConnected uses mode constants
--FILE--
<?php
$kafka = new Kafka('localhost:9092');
$connected = $kafka->isConnected(
    Kafka::MODE_PRODUCER
);
var_dump(
    $connected === $kafka->isConnected(Kafka::MODE_CONSUMER)
);
--EXPECT--
bool(true)
