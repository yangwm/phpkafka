--TEST--
Basic test for Kafka::OFFSET_END constant
--FILE--
<?php
var_dump(Kafka::OFFSET_END);
?>
--EXPECT--
string(3) "end"
