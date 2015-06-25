--TEST--
Basic test - Does the KafkaTopic class exist?
--FILE--
<?php
var_dump(
    class_exists('KafkaTopic')
);
?>
--EXPECT--
bool(true)
