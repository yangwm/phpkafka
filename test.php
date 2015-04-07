<?php
$kafka = new Kafka('localhost:9092');
$kafka->setLogLevel(Kafka::LOG_OFF);
var_dump($kafka->getPartitionsForTopic('test123'));
var_dump($kafka->getTopics());
for($i = 0; $i<2; $i++) {
    $kafka = new Kafka('localhost:9092');
    $kafka->produce("test123", $i);

}
