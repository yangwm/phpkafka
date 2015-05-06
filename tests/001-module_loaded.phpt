--TEST--
Basic test - Make sure the kafka extension is loaded
--FILE--
<?php
var_dump(
    in_array('kafka', get_loaded_extensions())
);
?>
--EXPECT--
bool(true)
