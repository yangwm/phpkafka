--TEST--
Basic test for Kafka::OFFSET_BEGIN constant
--FILE--
<?php
var_dump(Kafka::OFFSET_BEGIN);
?>
--EXPECT--
string(9) "beginning"
