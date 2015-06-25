/**
 *  Copyright 2015 Elias Van Ootegem.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * Special thanks to Patrick Reilly and Aleksandar Babic for their work
 * On which this extension was actually built.
 */
#ifndef PHP_KAFKA_H
#define	PHP_KAFKA_H 1

#define PHP_KAFKA_VERSION "0.2.2-dev"
#define PHP_KAFKA_EXTNAME "kafka"
#define PHP_KAFKA_OFFSET_BEGIN "beginning"
#define PHP_KAFKA_OFFSET_END "end"
#define PHP_KAFKA_LOG_ON 1
#define PHP_KAFKA_LOG_OFF 0
#define PHP_KAFKA_MODE_CONSUMER 0
#define PHP_KAFKA_MODE_PRODUCER 1
#define PHP_KAFKA_COMPRESSION_NONE "none"
#define PHP_KAFKA_COMPRESSION_GZIP "gzip"
#define PHP_KAFKA_COMPRESSION_SNAPPY "snappy"
//option constants...
#define PHP_KAFKA_RETRY_COUNT 1
#define PHP_KAFKA_RETRY_INTERVAL 2
#define PHP_KAFKA_CONFIRM_DELIVERY 4
#define PHP_KAFKA_QUEUE_BUFFER_SIZE 8
#define PHP_KAFKA_COMPRESSION_MODE 16
#define PHP_KAFKA_LOGLEVEL 32
#define PHP_KAFKA_PRODUCE_BATCH_SIZE 64
#define PHP_KAFKA_CONSUME_BATCH_SIZE 128
#define PHP_KAFKA_CONFIRM_OFF 0
#define PHP_KAFKA_CONFIRM_BASIC 1
#define PHP_KAFKA_CONFIRM_EXTENDED 2
extern zend_module_entry kafka_module_entry;

PHP_MSHUTDOWN_FUNCTION(kafka);
PHP_MINIT_FUNCTION(kafka);
PHP_RINIT_FUNCTION(kafka);
PHP_RSHUTDOWN_FUNCTION(kafka);
PHP_MINFO_FUNCTION(kafka);

#ifdef ZTS
#include <TSRM.h>
#endif
#include "librdkafka/rdkafka.h"

#define PHP_KAFKA_PARTITION_RANDOM RD_KAFKA_PARTITION_UA

//internal representation of Kafka instance
//this will! change, though, once the focus shifts towards KafkaTopic
typedef struct _kafka_r {
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
    long                consumer_partition;
    long                producer_partition;
    int                 log_level;
    rd_kafka_type_t     rk_type;
} kafka_connection;

//internal representation of KafkaTopic instance
typedef struct _kafka_topic_r {
    zend_object             std;
    char                    *topic_name;
    rd_kafka_t              *conn;
    rd_kafka_type_t         rk_type;
    rd_kafka_topic_t        *topic;
    rd_kafka_topic_conf_t   *config;
    rd_kafka_metadata_t     *meta;
} kafka_topic;

#define GET_KAFKA_CONNECTION(varname, thisObj) \
    kafka_connection *varname = (kafka_connection *) zend_object_store_get_object( \
        thisObj TSRMLS_CC \
    )

#define GET_KAFKA_TOPIC(varname, thisObj) \
    kafka_topic *varname = (kafka_topic *) zend_object_store_get_object( \
        thisObj TSRMLS_CC \
    )

/**
 * Special thanks to Kristina Chodorow for these macro's
 * borrowed from her excellent series PHP Extensions Made Eldrich
 *
 * See the full series here:
 * http://www.kchodorow.com/blog/2011/08/11/php-extensions-made-eldrich-installing-php/
 */

#define PUSH_PARAM(arg) zend_vm_stack_push(arg TSRMLS_CC)
#define POP_PARAM() (void)zend_vm_stack_pop(TSRMLS_C)
#define PUSH_EO_PARAM()
#define POP_EO_PARAM()

#define CALL_METHOD_BASE(classname, name) zim_##classname##_##name

#define CALL_METHOD_HELPER(classname, name, retval, thisptr, num, param) \
  PUSH_PARAM(param); PUSH_PARAM((void*)num);                            \
  PUSH_EO_PARAM();                                                      \
  CALL_METHOD_BASE(classname, name)(num, retval, NULL, thisptr, 0 TSRMLS_CC); \
  POP_EO_PARAM();                       \
  POP_PARAM(); POP_PARAM();

#define CALL_METHOD(classname, name, retval, thisptr)                  \
  CALL_METHOD_BASE(classname, name)(0, retval, NULL, thisptr, 0 TSRMLS_CC);

#define CALL_METHOD1(classname, name, retval, thisptr, param1)         \
  CALL_METHOD_HELPER(classname, name, retval, thisptr, 1, param1);

#define CALL_METHOD2(classname, name, retval, thisptr, param1, param2) \
  PUSH_PARAM(param1);                                                   \
  CALL_METHOD_HELPER(classname, name, retval, thisptr, 2, param2);     \
  POP_PARAM();

#define CALL_METHOD3(classname, name, retval, thisptr, param1, param2, param3) \
  PUSH_PARAM(param1); PUSH_PARAM(param2);                               \
  CALL_METHOD_HELPER(classname, name, retval, thisptr, 3, param3);     \
  POP_PARAM(); POP_PARAM();

//attach kafka connection to module
zend_object_value create_kafka_connection(zend_class_entry *class_type TSRMLS_DC);
//attach topic
zend_object_value create_kafka_topic(zend_class_entry *class_type TSRMLS_DC);

void free_kafka_connection(void *object TSRMLS_DC);
void free_kafka_topic(void *object TSRMLS_DC);

#endif
