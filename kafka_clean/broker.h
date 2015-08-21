#ifndef BROKER_H_
#define BROKER_H_

#include "librdkafka/rdkafka.h"
#include <php.h>

//constants
#define PHP_KAFKA_OFFSET_BEGIN "beginning"
#define PHP_KAFKA_OFFSET_END "end"
#define PHP_KAFKA_OFFSET_STORED "stored"
#define PHP_KAFKA_LOG_ON 1
#define PHP_KAFKA_LOG_OFF 0
#define PHP_KAFKA_MODE_CONSUMER 0
#define PHP_KAFKA_MODE_PRODUCER 1
#define PHP_KAFKA_COMPRESSION_NONE "none"
#define PHP_KAFKA_COMPRESSION_GZIP "gzip"
#define PHP_KAFKA_COMPRESSION_SNAPPY "snappy"
//option constants...
#define PHP_KAFKA_RETRY_COUNT 1
#define PHP_KAFKA_RETRY_INTERVAL (1 << 1)
#define PHP_KAFKA_CONFIRM_DELIVERY ( 1 << 2)
#define PHP_KAFKA_QUEUE_BUFFER_SIZE (1<<3)
#define PHP_KAFKA_COMPRESSION_MODE (1<<4)
#define PHP_KAFKA_LOGLEVEL (1<<5)
#define PHP_KAFKA_PRODUCE_BATCH_SIZE (1<<6)
#define PHP_KAFKA_CONSUME_BATCH_SIZE (1<<7)
#define PHP_KAFKA_CONFIRM_OFF 0
#define PHP_KAFKA_CONFIRM_BASIC 1
#define PHP_KAFKA_CONFIRM_EXTENDED 2
#define PHP_KAFKA_PARTITION_RANDOM RD_KAFKA_PARTITION_UA


extern zend_class_entry *broker_ce;

//MINIT function
void kafka_init_broker(INIT_FUNC_ARGS);

typedef struct _kafka_broker {
    zend_object         std;
    rd_kafka_t          *consumer;
    rd_kafka_t          *producer;
    char                *brokers;
    char                *compression;
    char                *retry_count;
    char                *retry_interval;
    int                 delivery_confirm_mode;
    char                *queue_buffer;
    long                consume_batch_size;
    long                produce_batch_size;
    int                 err_mode;
} kafka_connection;

#endif
