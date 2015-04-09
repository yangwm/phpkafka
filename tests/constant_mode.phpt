--TEST--
Basic test for Kafka::MODE_* constants
--FILE--
<?php
var_dump(Kafka::MODE_PRODUCER);
var_dump(Kafka::MODE_CONSUMER);
?>
--EXPECT--
int(1)
int(0)
