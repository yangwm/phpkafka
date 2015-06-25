<?php
final class KafkaTopic
{
    /**
     * internal property storing the name of the topic
     * @var string
     */
    private $name = null;

    /**
     * mode in which this instance operates Kafka::MODE_* constants
     * @var int
     */
    private $mode = null;

    /**
     * Can be used, but is discouraged, use Kafka::getTopic instead
     * The Kafka::getTopic method will connect if required
     * Calling the constructor directly (without valid connections) will throw exceptions
     * @param Kafka $connection
     * @param string $topicName
     * @param int $mode
     * @throws KafkaException
     */
    public function __construct(Kafka $connection, $topicName, $mode)
    {
        $this->name = $topicName;
        $this->mode = $mode;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }
}
