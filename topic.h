#ifndef PHP_KAFKA_TOPIC_H
#define PHP_KAFKA_TOPIC_H 1

#include "php_kafka.h"

typedef struct _kafka_topic_r {
    zend_object             std;
    rd_kafka_t              *connection;
    rd_kafka_type_t         rk_type;
    struct topic_list_node  *node;
    struct topic_list       *list;
} kafka_connection;

void kafka_init_topic_class(INIT_FUNC_ARGS);

#endif
