<?php
$kafka = new Kafka("localhost:9092");
$kafka->produce("test", "message yangwm");

$msg = $kafka->consume("test", Kafka::OFFSET_BEGIN, 20);
var_dump($msg);//dumps array of messages

