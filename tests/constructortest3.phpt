--TEST--
Test constructor arguments (config array passed as zval null)
--FILE--
<?php
$kafka = new Kafka('localhost:9092', null);
var_dump(get_class($kafka));
?>
--EXPECT--
string(5) "Kafka"
