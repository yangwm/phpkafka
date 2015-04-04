<?php

final class Kafka
{
    const OFFSET_BEGIN = 'beginning';
    const OFFSET_END = 'end';

    /**
     * This property does not exist, connection status
     * Is obtained directly from C kafka client
     * @var bool
     */
    private $connected = false;

    /**
     * @var int
     */
    private $partition = 0;

    public function __construct($brokers = 'localhost:9092')
    {}

    /**
     * @param int $partition
     * @deprecated use setPartition instead
     * @return $this
     */
    public function set_partition($partition)
    {
        $this->partition = $partition;
        return $this;
    }

    /**
     * @param int $partition
     * @return $this
     * @throws \Exception
     */
    public function setPartition($partition)
    {
        if (!is_int($partition)) {
            throw new \Exception(
                sprintf(
                    '%s expects argument to be an int',
                    __CLASS__
                )
            );
        }
        $this->partition = $partition;
        return $this;
    }

    /**
     * @param string $brokers
     * @return $this
     * @throws \Exception
     */
    public function setBrokers($brokers)
    {
        if (!is_string($brokers)) {
            throw new \Exception(
                sprintf(
                    '%s expects argument to be a string',
                    __CLASS__
                )
            );
        }
        $this->brokers = $brokers;
        return $this;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return $this->connected;
    }

    /**
     * produce message on topic
     * @param string $topic
     * @param string $message
     * @return $this
     */ 
    public function produce($topic, $message)
    {
        $this->connected = true;
        //internal call, produce message on topic
        return $this;
    }

    /**
     * @param string $topic
     * @param string|int $offset
     * @param string|int $count
     * @return array
     */
    public function consume($topic, $offset = self::OFFSET_BEGIN, $count = self::OFFSET_END)
    {
        $this->connected = true;
        $return = [];
        if (!is_numeric($offset)) {
            //0 or last message (whatever its offset might be)
            $start = $offset == self::OFFSET_BEGIN ? 0 : 100;
        } else {
            $start = $offset;
        }
        if (!is_numeric($count)) {
            //depending on amount of messages in topic
            $count = 100;
        }
        return array_fill_keys(
            range($start, $start + $count),
            'the message at the offset $key'
        );
    }

    /**
     * Returns an assoc array of topic names
     * The value is the partition count
     * @return array
     */
    public function getTopics()
    {
        return [
            'topicName' => 1
        ];
    }

    /**
     * Returns an array of ints (available partitions for topic)
     * @param string $topic
     * @return array
     */
    public function getPartitionsForTopic($topic)
    {
        return [];
    }

    public function __destruct()
    {
        $this->connected = false;
    }
}
