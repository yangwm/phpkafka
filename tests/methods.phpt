--TEST--
Test ensuring all methods exist (major pain in the backside when adding new methods...)
--FILE--
<?php
$kafka = new Kafka('localhost:9092');
var_dump(get_class_methods($kafka));
?>
--EXPECT--
array(20) {
  [0]=>
  string(11) "__construct"
  [1]=>
  string(10) "__destruct"
  [2]=>
  string(14) "setCompression"
  [3]=>
  string(14) "getCompression"
  [4]=>
  string(13) "set_partition"
  [5]=>
  string(12) "setPartition"
  [6]=>
  string(12) "getPartition"
  [7]=>
  string(11) "setLogLevel"
  [8]=>
  string(21) "getPartitionsForTopic"
  [9]=>
  string(19) "getPartitionOffsets"
  [10]=>
  string(10) "setBrokers"
  [11]=>
  string(10) "setOptions"
  [12]=>
  string(9) "getTopics"
  [13]=>
  string(10) "disconnect"
  [14]=>
  string(11) "isConnected"
  [15]=>
  string(7) "produce"
  [16]=>
  string(12) "produceBatch"
  [17]=>
  string(7) "consume"
  [18]=>
  string(12) "consumeBatch"
  [19]=>
  string(8) "getTopic"
}
