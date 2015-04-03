<?php
$kafka = new \Kafka("kafka-1:9092,kafka-2:9092");
$topic = 'eliasTest';
$msg = 'this message was produced on ' . date('Y-m-d H:i:s');
printf(
    'Producing message "%s" on topic "%s"' . PHP_EOL,
    $msg,
    $topic
);
$kafka->produce($topic, $msg);
echo 'Done', PHP_EOL;
echo 'Listing partitions for topic ', $topic, PHP_EOL;
var_dump($kafka->getPartitionsForTopic('eliasTest'));
echo 'Closing, and reopening connection', PHP_EOL;
$kafka = new \Kafka("kafka-1:9092,kafka-2:9092");
echo 'Done... Now selecting partition 2', PHP_EOL;
$kafka->setPartition(2);
$start = microtime(true);
$cons = $kafka->consume($topic, Kafka::OFFSET_BEGIN , Kafka::OFFSET_END);
$end = microtime(true);
$read = count($cons);
printf('Read all %d messages in %fms, or %f per message' . PHP_EOL,
    $read,
    $end - $start,
    $read ? ($end - $start)/$read : 0
);
echo 'Dumping messages';
var_dump($cons);
echo 'Closing connection', PHP_EOL;
$kafka->disconnect();
echo 'Done', PHP_EOL;
