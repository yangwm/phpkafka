#ifndef TOPIC_H_
#define TOPIC_H_

#include "librdkafka/rdkafka.h"
#include <php.h>

extern zend_class_entry *topic_ce;

void kafka_init_topic(INIT_FUNC_ARGS);

typedef struct _kafka_topic {
    zend_object                 std;
    char                        *topic_name;
    rd_kafka_t                  *conn;
    rd_kafka_type_t             rk_type;
    rd_kafka_topic_t            *topic;
    rd_kafka_topic_conf_t       *config;
    const rd_kafka_metadata_t   *meta;
} kafka_topic;

#endif
