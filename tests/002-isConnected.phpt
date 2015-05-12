--TEST--
Basic test - Kafka::isConnected can handle zval null's, too
--FILE--
<?php
$kafka = new Kafka('localhost:9092');
$connected = $kafka->isConnected();
var_dump(
    $connected === $kafka->isConnected(null)
);
--EXPECT--
bool(true)
