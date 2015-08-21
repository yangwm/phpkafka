#ifndef QUEUE_H_
#define QUEUE_H_

#include "librdkafka/rdkafka.h"
#include <php.h>

extern zend_class_entry *queue_ce;

void kafka_init_queue(INIT_FUNC_ARGS);

typedef struct _kafka_queue {
    zend_object             std;
    zval                    *msg_arr;
    long                    batch_size;
    long                    item_count;
    zend_bool               is_done;
    char                    error_msg[512];
    int                     *partition_ends;
    int                     eop;
} kafka_queue;

#endif
