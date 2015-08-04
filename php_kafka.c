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

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <php.h>
#include <php_kafka.h>
#include "kafka.h"
#include "zend_exceptions.h"
#include "zend_hash.h"
#include <zlib.h>
#include <ctype.h>

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif
#define REGISTER_KAFKA_CLASS_CONST_STRING(ce, name, value) \
    zend_declare_class_constant_stringl(ce, name, sizeof(name)-1, value, sizeof(value)-1)
#define REGISTER_KAFKA_CLASS_CONST_LONG(ce, name, value) \
    zend_declare_class_constant_long(ce, name, sizeof(name)-1, value)
#define REGISTER_KAFKA_CLASS_CONST(ce, c_name, type) \
    REGISTER_KAFKA_CLASS_CONST_ ## type(ce, #c_name, PHP_KAFKA_ ## c_name)
#ifndef BASE_EXCEPTION
#if (PHP_MAJOR_VERSION < 5) || ( ( PHP_MAJOR_VERSION == 5 ) && (PHP_MINOR_VERSION < 2) )
#define BASE_EXCEPTION zend_exception_get_default()
#else
#define BASE_EXCEPTION zend_exception_get_default(TSRMLS_C)
#endif
#endif

/* decalre the class entries */
zend_class_entry *kafka_ce;
zend_class_entry *kafka_topic_ce;
zend_class_entry *kafka_exception;
zend_class_entry *kafka_queue_ce;

/* We don't want to allow clone for any of our objects */
static zend_object_handlers kafka_handlers;


/* {{{ arginfo Kafka */
ZEND_BEGIN_ARG_INFO_EX(arginf_kafka__constr, 0, 0, 1)
    ZEND_ARG_INFO(0, brokers)
    ZEND_ARG_INFO(0, options)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_set_options, 0)
    ZEND_ARG_INFO(0, options)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_set_partition, 0, 0, 1)
    ZEND_ARG_INFO(0, partition)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_set_compression, 0)
    ZEND_ARG_INFO(0, compression)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_set_log_level, 0)
    ZEND_ARG_INFO(0, logLevel)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_get_partitions_for_topic, 0)
    ZEND_ARG_INFO(0, topic)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_set_get_partition, 0)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_produce, 0)
    ZEND_ARG_INFO(0, topic)
    ZEND_ARG_INFO(0, message)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_produce_batch, 0, 0, 2)
    ZEND_ARG_INFO(0, topic)
    ZEND_ARG_INFO(0, messages)
    ZEND_ARG_INFO(0, batchSize)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_consume, 0, 0, 2)
    ZEND_ARG_INFO(0, topic)
    ZEND_ARG_INFO(0, offset)
    ZEND_ARG_INFO(0, messageCount)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_consume_batch, 0)
    ZEND_ARG_INFO(0, topic)
    ZEND_ARG_INFO(0, offset)
    ZEND_ARG_INFO(0, batchSize)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_is_conn, 0, 0, 0)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_void, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_disconnect, 0, 0, 0)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_get_topic, 0)
    ZEND_ARG_INFO(0, topicName)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

/* }}} end arginfo Kafka*/
/* {{{ Kafka methods declaration */
static PHP_METHOD(Kafka, __construct);
static PHP_METHOD(Kafka, __destruct);
static PHP_METHOD(Kafka, setCompression);
static PHP_METHOD(Kafka, getCompression);
static PHP_METHOD(Kafka, set_partition);
static PHP_METHOD(Kafka, setPartition);
static PHP_METHOD(Kafka, getPartition);
static PHP_METHOD(Kafka, setLogLevel);
static PHP_METHOD(Kafka, getPartitionsForTopic);
static PHP_METHOD(Kafka, getPartitionOffsets);
static PHP_METHOD(Kafka, isConnected);
static PHP_METHOD(Kafka, setBrokers);
static PHP_METHOD(Kafka, setOptions);
static PHP_METHOD(Kafka, getTopics);
static PHP_METHOD(Kafka, disconnect);
static PHP_METHOD(Kafka, produceBatch);
static PHP_METHOD(Kafka, produce);
static PHP_METHOD(Kafka, consume);
static PHP_METHOD(Kafka, consumeBatch);
static PHP_METHOD(Kafka, getTopic);
/* }}} end Kafka methods */

/* {{{ arginfo KafkaTopic */
ZEND_BEGIN_ARG_INFO(arginf_kafka_topic__constr, 0)
    ZEND_ARG_INFO(0, connection)
    ZEND_ARG_INFO(0, topicName)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_topic_void, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_topic_produce, 0)
    ZEND_ARG_INFO(0, message)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_topic_produce_batch, 0, 0, 1)
    ZEND_ARG_INFO(0, messages)
    ZEND_ARG_INFO(0, batchSize)
    ZEND_ARG_INFO(0, blocking)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_topic_consume, 0, 0, 0)
    ZEND_ARG_INFO(0, messageCount)
    ZEND_ARG_INFO(0, offset)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_topic_consume_batch, 0)
    ZEND_ARG_INFO(0, batchSize)
    ZEND_ARG_INFO(0, offset)
ZEND_END_ARG_INFO()

/* }}} end arginfo KafkaTopic */

/* {{{ KafkaTopic method declaration */
static PHP_METHOD(KafkaTopic, __construct);
static PHP_METHOD(KafkaTopic, getName);
static PHP_METHOD(KafkaTopic, getPartitionCount);
static PHP_METHOD(KafkaTopic, produce);
static PHP_METHOD(KafkaTopic, produceBatch);
static PHP_METHOD(KafkaTopic, consume);
static PHP_METHOD(KafkaTopic, consumeBatch);
/* }}} end KafkaTopic methods */

/* {{{ arginfo KafkaQueue */
ZEND_BEGIN_ARG_INFO(arginf_kafka_queue_void, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_queue_get_messages, 0, 0, 0)
    ZEND_ARG_INFO(0, andStop)
ZEND_END_ARG_INFO()
/* }}} end KafkaQueue arginfo */

/* {{{ KafkaQueue method decl */
static PHP_METHOD(KafkaQueue, getStatus);
static PHP_METHOD(KafkaQueue, getMessages);
static PHP_METHOD(KafkaQueue, stop);
static PHP_METHOD(KafkaQueue, pauze);
static PHP_METHOD(KafkaQueue, resume);
static PHP_METHOD(KafkaQueue, __destruct);
/* }}} end KafkaQueue methods */

/* {{{ Method tables */

//KafkaQueue
static zend_function_entry kafka_queue_functions[] = {
    PHP_ME(KafkaQueue, getStatus, arginf_kafka_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaQueue, getMessages, arginf_kafka_queue_get_messages, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaQueue, stop, arginf_kafka_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaQueue, pauze, arginf_kafka_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaQueue, resume, arginf_kafka_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaQueue, __destruct, arginf_kafka_queue_void, ZEND_ACC_PUBLIC)
    {NULL,NULL,NULL}
};

//KafkaTopic
static zend_function_entry kafka_topic_functions[] = {
    PHP_ME(KafkaTopic, __construct, arginf_kafka_topic__constr, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, getName, arginf_kafka_topic_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, getPartitionCount, arginf_kafka_topic_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, produce, arginf_kafka_topic_produce, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, produceBatch, arginf_kafka_topic_produce_batch, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, consume, arginf_kafka_topic_consume, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, consumeBatch, arginf_kafka_topic_consume_batch, ZEND_ACC_PUBLIC)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};

//Kafka
static zend_function_entry kafka_functions[] = {
    PHP_ME(Kafka, __construct, arginf_kafka__constr, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, __destruct, arginf_kafka_void, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setCompression, arginf_kafka_set_compression, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getCompression, arginf_kafka_void, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, set_partition, arginf_kafka_set_partition, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
    PHP_ME(Kafka, setPartition, arginf_kafka_set_partition, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartition, arginf_kafka_set_get_partition, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setLogLevel, arginf_kafka_set_log_level, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartitionsForTopic, arginf_kafka_get_partitions_for_topic, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartitionOffsets, arginf_kafka_get_partitions_for_topic, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setBrokers, arginf_kafka__constr, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setOptions, arginf_kafka_set_options, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getTopics, arginf_kafka_void, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, disconnect, arginf_kafka_disconnect, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, isConnected, arginf_kafka_is_conn, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, produce, arginf_kafka_produce, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, produceBatch, arginf_kafka_produce_batch, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, consume, arginf_kafka_consume, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, consumeBatch, arginf_kafka_consume_batch, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getTopic, arginf_kafka_get_topic, ZEND_ACC_PUBLIC)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};

zend_module_entry kafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "kafka",
    kafka_functions, /* Function entries */
    PHP_MINIT(kafka), /* Module init */
    PHP_MSHUTDOWN(kafka), /* Module shutdown */
    PHP_RINIT(kafka), /* Request init */
    PHP_RSHUTDOWN(kafka), /* Request shutdown */
    PHP_MINFO(kafka), /* Module information */
    PHP_KAFKA_VERSION, /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

PHP_MINIT_FUNCTION(kafka)
{
    zend_class_entry ce,
            ce_ex,
            ce_t;
    //setup the default kafka handlers
    memcpy(
        &kafka_handlers,
        zend_get_std_object_handlers(),
        sizeof kafka_handlers
    );
    //disable cloning!
    kafka_handlers.clone_obj = NULL;

    //register Kafka class
    INIT_CLASS_ENTRY(ce, "Kafka", kafka_functions);
    kafka_ce = zend_register_internal_class(&ce TSRMLS_CC);
    //register KafkaException class, extends BASE_EXCEPTION
    INIT_CLASS_ENTRY(ce_ex, "KafkaException", NULL);
    kafka_exception = zend_register_internal_class_ex(
        &ce_ex,
        BASE_EXCEPTION,
        NULL TSRMLS_CC
    );
    //register KafkaTopic class
    INIT_CLASS_ENTRY(ce_t, "KafkaTopic", kafka_topic_functions);
    kafka_topic_ce = zend_register_internal_class(&ce_t TSRMLS_CC);
    //add create_object handler & make final
    kafka_topic_ce->create_object = create_kafka_topic;
    kafka_topic_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;

    //do not allow people to extend this class, make it final
    kafka_ce->create_object = create_kafka_connection;
    kafka_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
    //offset constants (consume)
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, OFFSET_BEGIN, STRING);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, OFFSET_END, STRING);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, OFFSET_STORED, STRING);
    //compression mode constants
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, COMPRESSION_NONE, STRING);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, COMPRESSION_GZIP, STRING);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, COMPRESSION_SNAPPY, STRING);
    //global log-mode constants TODO: refactor to ERRMODE constants
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, LOG_ON, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, LOG_OFF, LONG);
    //connection mode constants
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, MODE_CONSUMER, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, MODE_PRODUCER, LONG);
    //random partition constant
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, PARTITION_RANDOM, LONG);
    //config constants:
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, RETRY_COUNT, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, RETRY_INTERVAL, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, CONFIRM_DELIVERY, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, QUEUE_BUFFER_SIZE, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, COMPRESSION_MODE, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, LOGLEVEL, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, PRODUCE_BATCH_SIZE, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, CONSUME_BATCH_SIZE, LONG);
    //confirmation value constants
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, CONFIRM_OFF, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, CONFIRM_BASIC, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, CONFIRM_EXTENDED, LONG);
    return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(kafka)
{
    return SUCCESS;
}

PHP_RINIT_FUNCTION(kafka)
{
    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(kafka)
{
    return SUCCESS;
}

PHP_MINFO_FUNCTION(kafka)
{
    char buffer[50];
    snprintf(
        buffer,
        50,
        "librdkafka version: %x.%x.%x.%x",
        (RD_KAFKA_VERSION & 0xFF000000) >> 24, //major
        (RD_KAFKA_VERSION & 0x00FF0000) >> 16, //minor
        (RD_KAFKA_VERSION & 0x0000FF00) >> 8,  //revision
        (RD_KAFKA_VERSION & 0x000000FF)        //unused ATM
    );
    php_info_print_table_start();
    php_info_print_table_header(2, "Directive", "Value");
    php_info_print_table_row(2, "Kafka enabled", "True");
    php_info_print_table_row(2, "Kafka extension version", PHP_KAFKA_VERSION);
    php_info_print_table_row(2, "Kafka support", "version <= 0.8");
    php_info_print_table_row(2, "Kafka C-Client", buffer);
    php_info_print_table_row(2, "ZooKeeper support", "Not supported");
    php_info_print_table_row(2, "Compression support", "Gzip, Snappy");
    php_info_print_table_end();
    //not just yet:
    //DISPLAY_INI_ENTRIES();
}
zend_object_value create_kafka_topic(zend_class_entry *class_type TSRMLS_DC)
{
    zend_object_value retval;
    kafka_topic *intern;
    zval *tmp;
    intern = emalloc(sizeof *intern);
    memset(intern, 0, sizeof *intern);
    zend_object_std_init(&intern->std, class_type TSRMLS_CC);
#if PHP_VERSION_ID < 50399
    zend_hash_copy(
        intern->std.properties, &class_type->default_properties,
        (copy_ctor_func_t)zval_add_ref,
        (void *)&tmp,
        sizeof tmp
    );
#else
    object_properties_init(&intern->std, class_type);
#endif

    // create a destructor for this struct
    retval.handle = zend_objects_store_put(
        intern,
        (zend_objects_store_dtor_t) zend_objects_destroy_object,
        free_kafka_topic,
        NULL TSRMLS_CC
    );
    retval.handlers = &kafka_handlers;

    return retval;
}

zend_object_value create_kafka_queue(zend_class_entry *class_type TSRMLS_DC)
{
    zend_object_value retval;
    kafka_queue *q_intern;
    zval *tmp;

    q_intern = emalloc(sizeof *q_intern);
    memset(q_intern, 0, sizeof *q_intern);
    q_intern->params.batch_size = 0;
    q_intern->status = PHP_KAFKA_QUEUE_IDLE;
    ALLOC_ZVAL(q_intern->params.msg_arr);
    INIT_ZVAL(*q_intern->params.msg_arr);
    array_init(q_intern->params.msg_arr);

    zend_object_std_init(&q_intern->std, class_type TSRMLS_CC);
#if PHP_VERSION_ID < 50399
    zend_hash_copy(
        q_intern->std.properties, &class_type->default_properties,
        (copy_ctor_func_t)zval_add_ref,
        (void *)&tmp,
        sizeof tmp
    );
#else
    object_properties_init(&q_intern->std, class_type);
#endif
    // create a destructor for this struct
    retval.handle = zend_objects_store_put(
        q_intern,
        (zend_objects_store_dtor_t) zend_objects_destroy_object,
        free_kafka_queue,
        NULL TSRMLS_CC
    );
    retval.handlers = &kafka_handlers;
    return retval;
}

zend_object_value create_kafka_connection(zend_class_entry *class_type TSRMLS_DC)
{
    zend_object_value retval;
    kafka_connection *intern;
    zval *tmp;

    // allocate the struct we're going to use
    intern = emalloc(sizeof *intern);
    memset(intern, 0, sizeof *intern);
    //init partitions to random partitions
    intern->consumer_partition = PHP_KAFKA_PARTITION_RANDOM;
    intern->producer_partition = PHP_KAFKA_PARTITION_RANDOM;
    //set default values
    //basic confirmation (wait for success callback)
    intern->delivery_confirm_mode = PHP_KAFKA_CONFIRM_BASIC;
    //logging = default on (while in development, at least)
    intern->log_level = PHP_KAFKA_LOG_ON;
    //init batch sizes to 50
    intern->produce_batch_size = intern->consume_batch_size = 50;

    zend_object_std_init(&intern->std, class_type TSRMLS_CC);
    //add properties table
#if PHP_VERSION_ID < 50399
    zend_hash_copy(
        intern->std.properties, &class_type->default_properties,
        (copy_ctor_func_t)zval_add_ref,
        (void *)&tmp,
        sizeof tmp
    );
#else
    object_properties_init(&intern->std, class_type);
#endif

    // create a destructor for this struct
    retval.handle = zend_objects_store_put(
        intern,
        (zend_objects_store_dtor_t) zend_objects_destroy_object,
        free_kafka_connection,
        NULL TSRMLS_CC
    );
    retval.handlers = &kafka_handlers;

    return retval;
}

//dtor handle for KafkaQueue
void free_kafka_queue(void *object TSRMLS_DC)
{
    kafka_queue *queue = (kafka_queue *) object;
    if (queue->queue)
    {
        //@todo: close queue with info in topic_ref
    }
    if (queue->topic_ref)
    {//remove reference, so topic can be GC'ed
        Z_DELREF_P(queue->topic_ref);
        //gone
        queue->topic_ref = NULL;
    }
    efree(queue);
}

//dtor handle for KafkaTopic instances... needs some work, though...
void free_kafka_topic(void *obj TSRMLS_DC)
{
    kafka_topic *topic = (kafka_topic *) obj;
    if (topic->topic_name) {
        efree(topic->topic_name);
    }
    if (topic->conn != NULL)
    {
        //kill the handle, quite brutally
        destroy_kafka_topic_handle(
            topic->conn,
            topic->topic,
            topic->config,
            topic->meta,
            -1
        );
    }

    efree(topic);
}

//clean current connections
void free_kafka_connection(void *object TSRMLS_DC)
{
    int interval = 1;
    kafka_connection *connection = ((kafka_connection *) object);
    //no confirmation, wait to close connection a bit longer, for what it's worth
    if (connection->delivery_confirm_mode == PHP_KAFKA_CONFIRM_OFF)
        interval = 50;

    if (connection->brokers)
        efree(connection->brokers);
    if (connection->compression)
        efree(connection->compression);
    if (connection->queue_buffer)
        efree(connection->queue_buffer);
    if (connection->retry_count)
        efree(connection->retry_count);
    if (connection->retry_interval)
        efree(connection->retry_interval);
    if (connection->consumer != NULL)
        kafka_destroy(
            connection->consumer,
            1
        );
    if (connection->producer != NULL)
        kafka_destroy(
            connection->producer,
            interval
        );
    efree(connection);
}

static
int is_number(const char *str)
{
    while (*str != '\0')
    {
        if (!isdigit(*str))
            return 0;
        ++str;
    }
    return 1;
}

//parse connection config array, and update connection struct
static int parse_options_array(zval *arr, kafka_connection **conn)
{
    zval **entry;
    char *assoc_key;
    int key_len;
    long idx;
    HashPosition pos;
    //make life easier, dereference struct
    kafka_connection *connection = *conn;
    zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(arr), &pos);
    while (zend_hash_get_current_data_ex(Z_ARRVAL_P(arr), (void **)&entry, &pos) == SUCCESS)
    {
        if (zend_hash_get_current_key_ex(Z_ARRVAL_P(arr), &assoc_key, &key_len, &idx, 0, &pos) == HASH_KEY_IS_STRING)
        {
            zend_throw_exception(kafka_exception, "Invalid option key, use class constants", 0 TSRMLS_CC);
            return -1;
        }
        else
        {
            char tmp[128];
            switch (idx)
            {
                case PHP_KAFKA_PRODUCE_BATCH_SIZE:
                    if (Z_TYPE_PP(entry) == IS_STRING)
                    {//if numeric string is passed, attempt to convert it to int
                        convert_to_long_ex(entry);
                    }
                    if (Z_TYPE_PP(entry) != IS_LONG)
                    {
                        zend_throw_exception(kafka_exception, "Invalid argument for Kafka::PRODUCE_BATCH_SIZE, expected numeric value", 0 TSRMLS_CC);
                        return -1;
                    }
                    connection->produce_batch_size = Z_LVAL_PP(entry);
                    break;
                case PHP_KAFKA_CONSUME_BATCH_SIZE:
                    if (Z_TYPE_PP(entry) == IS_STRING)
                    {
                        convert_to_long_ex(entry);
                    }
                    if (Z_TYPE_PP(entry) != IS_LONG)
                    {
                        zend_throw_exception(kafka_exception, "Invalid argument for Kafka::CONSUME_BATCH_SIZE, expected numeric value", 0 TSRMLS_CC);
                        return -1;
                    }
                    connection->consume_batch_size = Z_LVAL_PP(entry);
                    break;
                case PHP_KAFKA_RETRY_COUNT:
                    if (Z_TYPE_PP(entry) == IS_STRING && is_number(Z_STRVAL_PP(entry)))
                    {
                        if (connection->retry_count)
                            efree(connection->retry_count);
                        connection->retry_count = estrdup(Z_STRVAL_PP(entry));
                    }
                    else if (Z_TYPE_PP(entry) == IS_LONG)
                    {
                        if (connection->retry_count)
                            efree(connection->retry_count);
                        snprintf(tmp, 128, "%d", Z_LVAL_PP(entry));
                        connection->retry_count = estrdup(tmp);
                    }
                    else
                    {
                        zend_throw_exception(kafka_exception, "Invalid value for Kafka::RETRY_COUNT option, expected numeric value", 0 TSRMLS_CC);
                        return -1;
                    }
                    break;
                case PHP_KAFKA_RETRY_INTERVAL:
                    if (Z_TYPE_PP(entry) == IS_STRING && is_number(Z_STRVAL_PP(entry)))
                    {
                        if (connection->retry_interval)
                            efree(connection->retry_interval);
                        connection->retry_interval = estrdup(Z_STRVAL_PP(entry));
                    }
                    else if (Z_TYPE_PP(entry) == IS_LONG)
                    {
                        if (connection->retry_interval)
                            efree(connection->retry_interval);
                        snprintf(tmp, 128, "%d", Z_LVAL_PP(entry));
                        connection->retry_interval = estrdup(tmp);
                    }
                    else
                    {
                        zend_throw_exception(kafka_exception, "Invalid value for Kafka::RETRY_INTERVAL option, expected numeric value", 0 TSRMLS_CC);
                        return -1;
                    }
                    break;
                case PHP_KAFKA_CONFIRM_DELIVERY:
                    if (
                        Z_TYPE_PP(entry) != IS_LONG
                        ||
                        (
                            Z_LVAL_PP(entry) != PHP_KAFKA_CONFIRM_OFF
                            &&
                            Z_LVAL_PP(entry) != PHP_KAFKA_CONFIRM_BASIC
                            &&
                            Z_LVAL_PP(entry) != PHP_KAFKA_CONFIRM_EXTENDED
                        )
                    )
                    {
                        zend_throw_exception(kafka_exception, "Invalid value for Kafka::CONFIRM_DELIVERY, use Kafka::CONFIRM_* constants", 0 TSRMLS_CC);
                        return -1;
                    }
                    connection->delivery_confirm_mode = Z_LVAL_PP(entry);
                    break;
                case PHP_KAFKA_QUEUE_BUFFER_SIZE:
                    if (Z_TYPE_PP(entry) == IS_STRING && is_number(Z_STRVAL_PP(entry)))
                    {
                        if (connection->queue_buffer)
                            efree(connection->queue_buffer);
                        connection->queue_buffer = estrdup(Z_STRVAL_PP(entry));
                    }
                    else if (Z_TYPE_PP(entry) == IS_LONG)
                    {
                        if (connection->queue_buffer)
                            efree(connection->queue_buffer);
                        snprintf(tmp, 128, "%d", Z_LVAL_PP(entry));
                        connection->queue_buffer = estrdup(tmp);
                    }
                    else
                    {
                        zend_throw_exception(kafka_exception, "Invalid value for Kafka::QUEUE_BUFFER_SIZE, expected numeric value", 0 TSRMLS_CC);
                        return -1;
                    }
                    break;
                case PHP_KAFKA_COMPRESSION_MODE:
                    if (Z_TYPE_PP(entry) != IS_STRING)
                    {
                        zend_throw_exception(kafka_exception, "Invalid type for Kafka::COMPRESSION_MODE option, use Kafka::COMPRESSION_* constants", 0 TSRMLS_CC);
                        return -1;
                    }
                    if (
                        !strcmp(Z_STRVAL_PP(entry), PHP_KAFKA_COMPRESSION_GZIP)
                        &&
                        !strcmp(Z_STRVAL_PP(entry), PHP_KAFKA_COMPRESSION_NONE)
                        &&
                        !strcmp(Z_STRVAL_PP(entry), PHP_KAFKA_COMPRESSION_SNAPPY)
                    ) {
                        zend_throw_exception(kafka_exception, "Invalid value for Kafka::COMPRESSION_MODE, use Kafka::COMPRESSION_* constants", 0 TSRMLS_CC);
                        return -1;
                    }
                    if (connection->compression)
                        efree(connection->compression);
                    connection->compression = estrdup(Z_STRVAL_PP(entry));
                    break;
                case PHP_KAFKA_LOGLEVEL:
                    if (Z_TYPE_PP(entry) != IS_LONG ||
                        (Z_LVAL_PP(entry) != PHP_KAFKA_LOG_OFF && Z_LVAL_PP(entry) != PHP_KAFKA_LOG_ON))
                    {
                        zend_throw_exception(kafka_exception, "Invalid value for Kafka::LOGLEVEL option, use Kafka::LOG_* constants", 0 TSRMLS_CC);
                        return -1;
                    }
                    connection->log_level = Z_LVAL_PP(entry);
                    break;
            }
        }
        zend_hash_move_forward_ex(Z_ARRVAL_P(arr), &pos);
    }
    return 0;
}

/** {{{ proto void DOMDocument::__construct( string $brokers [, array $options = null]);
    Constructor, expects a comma-separated list of brokers to connect to
*/
PHP_METHOD(Kafka, __construct)
{
    zval *arr = NULL;
    char *brokers = NULL;
    int brokers_len = 0;
    kafka_connection *connection = (kafka_connection *) zend_object_store_get_object(
        getThis() TSRMLS_CC
    );

    //force constructor to throw exceptions in case of an error
    zend_replace_error_handling(EH_THROW, kafka_exception, NULL TSRMLS_CC);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|a!",
            &brokers, &brokers_len, &arr) == SUCCESS)
    {
        if (arr && parse_options_array(arr, &connection))
        {//if above is true, an array was passed, but it was invalid
            //restore normal error handling
            zend_replace_error_handling(EH_NORMAL, NULL, NULL TSRMLS_CC);
            return;//we've thrown an exception
        }
        connection->brokers = estrdup(brokers);
        kafka_set_log_level(connection->log_level);
        kafka_connect(brokers);
    }
    zend_replace_error_handling(EH_NORMAL, NULL, NULL TSRMLS_CC);
}
/* }}} end Kafka::__construct */

/* {{{ proto bool Kafka::isConnected( [ int $mode ] )
    returns true if kafka connection is active, fals if not
    Mode defaults to current active mode
*/
PHP_METHOD(Kafka, isConnected)
{
    zval *mode = NULL,
        *obj = getThis();
    long tmp_val = -1;
    rd_kafka_type_t type;
    GET_KAFKA_CONNECTION(k, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z!", &mode) == FAILURE)
        return;
    if (mode)
    {
        if (Z_TYPE_P(mode) == IS_LONG)
            tmp_val = Z_LVAL_P(mode);
        if (tmp_val != PHP_KAFKA_MODE_CONSUMER && tmp_val != PHP_KAFKA_MODE_PRODUCER)
        {
            zend_throw_exception(
                kafka_exception,
                "invalid argument passed to Kafka::isConnected, use Kafka::MODE_* constants",
                 0 TSRMLS_CC
            );
            return;
        }
        if (tmp_val == PHP_KAFKA_MODE_CONSUMER)
            type = RD_KAFKA_CONSUMER;
        else
            type = RD_KAFKA_PRODUCER;
    }
    else
        type = k->rk_type;
    if (type == RD_KAFKA_CONSUMER)
    {
        if (k->consumer != NULL)
        {
            RETURN_TRUE;
        }
        RETURN_FALSE;

    }
    if (k->producer != NULL)
    {
        RETURN_TRUE;
    }
    RETURN_FALSE;
}
/* }}} end bool Kafka::isConnected */

/* {{{ proto void Kafka::__destruct( void )
    constructor, disconnects kafka
*/
PHP_METHOD(Kafka, __destruct)
{
    int interval = 1;
    kafka_connection *connection = (kafka_connection *) zend_object_store_get_object(
        getThis() TSRMLS_CC
    );
    if (connection->delivery_confirm_mode == PHP_KAFKA_CONFIRM_OFF)
        interval = 25;
    if (connection->brokers)
        efree(connection->brokers);
    if (connection->queue_buffer)
        efree(connection->queue_buffer);
    if (connection->retry_count)
        efree(connection->retry_count);
    if (connection->retry_interval)
        efree(connection->retry_interval);
    if (connection->compression)
        efree(connection->compression);
    if (connection->consumer != NULL)
        kafka_destroy(
            connection->consumer,
            1
        );
    if (connection->producer != NULL)
        kafka_destroy(
            connection->producer,
            interval
        );
    connection->producer    = NULL;
    connection->brokers     = NULL;
    connection->compression = NULL;
    connection->consumer    = NULL;
    connection->queue_buffer = connection->retry_count = connection->retry_interval = NULL;
    connection->delivery_confirm_mode = 0;
    connection->consumer_partition = connection->producer_partition = PHP_KAFKA_PARTITION_RANDOM;
}
/* }}} end Kafka::__destruct */

/* {{{ proto Kafka Kafka::set_partition( int $partition [, int $mode ] );
    Set partition (used by consume method)
    This method is deprecated, in favour of the more PSR-compliant
    Kafka::setPartition
*/
PHP_METHOD(Kafka, set_partition)
{
    zval *partition,
        *mode = NULL,
        *obj = getThis();
    long p_value;
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z!", &partition, &mode) == FAILURE)
        return;
    if (Z_TYPE_P(partition) != IS_LONG || (mode && Z_TYPE_P(mode) != IS_LONG)) {
        zend_throw_exception(kafka_exception, "Partition and/or mode is expected to be an int", 0 TSRMLS_CC);
        return;
    }
    if (mode)
    {
        if (Z_LVAL_P(mode) != PHP_KAFKA_MODE_CONSUMER && Z_LVAL_P(mode) != PHP_KAFKA_MODE_PRODUCER)
        {
            zend_throw_exception(
                kafka_exception,
                "invalid mode argument passed to Kafka::setPartition, use Kafka::MODE_* constants",
                0 TSRMLS_CC
            );
            return;
        }
    }
    p_value = Z_LVAL_P(partition);
    if (p_value < -1)
    {
        zend_throw_exception(
            kafka_exception,
            "invalid partition passed to Kafka::setPartition, partition value should be >= 0 or Kafka::PARTION_RANDOM",
            0 TSRMLS_CC
        );
        return;
    }
    p_value = p_value == -1 ? PHP_KAFKA_PARTITION_RANDOM : p_value;
    if (!mode)
    {
        connection->consumer_partition = p_value;
        connection->producer_partition = p_value;
        kafka_set_partition(p_value);
    }
    else
    {
        if (Z_LVAL_P(mode) != PHP_KAFKA_MODE_CONSUMER)
            connection->producer_partition = p_value;
        else
            connection->consumer_partition = p_value;
    }
    //return $this
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::set_partition */

/* {{{ proto Kafka Kafka::setLogLevel( mixed $logLevel )
    toggle syslogging on or off use Kafka::LOG_* constants
*/
PHP_METHOD(Kafka, setLogLevel)
{
    zval *log_level,
        *obj = getThis();
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &log_level) == FAILURE)
    {
        return;//?
    }
    if (Z_TYPE_P(log_level) != IS_LONG) {
        zend_throw_exception(kafka_exception, "Kafka::setLogLevel expects argument to be an int", 0 TSRMLS_CC);
        return;
    }
    if (
        Z_LVAL_P(log_level) != PHP_KAFKA_LOG_ON
        &&
        Z_LVAL_P(log_level) != PHP_KAFKA_LOG_OFF
    ) {
        zend_throw_exception(kafka_exception, "Invalid argument, use Kafka::LOG_* constants", 0 TSRMLS_CC);
        return;
    }
    connection->log_level = Z_LVAL_P(log_level);
    kafka_set_log_level(connection->log_level);
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::setLogLevel */

/* {{{ proto Kafka Kafka::setCompression( string $compression )
 * Enable compression for produced messages
 */
PHP_METHOD(Kafka, setCompression)
{
    zval *obj = getThis();
    char *arg;
    int arg_len;
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &arg, &arg_len) == FAILURE)
    {
        return;
    }
    //if valid compression constant was used...
    if (
        !strcmp(arg, PHP_KAFKA_COMPRESSION_GZIP)
            ||
        !strcmp(arg, PHP_KAFKA_COMPRESSION_NONE)
            ||
        !strcmp(arg, PHP_KAFKA_COMPRESSION_SNAPPY)
    )
    {
        if (connection->compression || strcmp(connection->compression, arg))
        {
            //close connections, if any, currently only use compression for producers
            if (connection->producer)
                kafka_destroy(connection->producer, 1);
            connection->producer = NULL;
            connection->producer_partition = PHP_KAFKA_PARTITION_RANDOM;
            connection->compression = estrdup(arg);
        }
    }
    else
    {
        zend_throw_exception(kafka_exception, "Invalid argument, use Kafka::COMPRESSION_* constants", 0 TSRMLS_CC);
    }
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto Kafka::setCompression */

/* {{{ proto string Kafka::getCompression( void )
 * Get type of compression that is currently used
 */
PHP_METHOD(Kafka, getCompression)
{
    zval *obj = getThis();
    GET_KAFKA_CONNECTION(connection, obj);
    if (!connection->compression)
        RETURN_STRING(PHP_KAFKA_COMPRESSION_NONE, 1);
    RETURN_STRING(connection->compression, 1);
}
/* }}} end proto Kafka::getCompression */

/* {{{ proto Kafka Kafka::setPartition( int $partition [, int $mode ] );
    Set partition to use for Kafka::consume calls
*/
PHP_METHOD(Kafka, setPartition)
{
    zval *partition,
        *mode = NULL,
        *obj = getThis();
    long p_value;
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z!", &partition, &mode) == FAILURE)
        return;
    if (Z_TYPE_P(partition) != IS_LONG || (mode && Z_TYPE_P(mode) != IS_LONG)) {
        zend_throw_exception(kafka_exception, "Partition and/or mode is expected to be an int", 0 TSRMLS_CC);
        return;
    }
    if (mode)
    {
        if (Z_LVAL_P(mode) != PHP_KAFKA_MODE_CONSUMER && Z_LVAL_P(mode) != PHP_KAFKA_MODE_PRODUCER)
        {
            zend_throw_exception(
                kafka_exception,
                "invalid mode argument passed to Kafka::setPartition, use Kafka::MODE_* constants",
                0 TSRMLS_CC
            );
            return;
        }
    }
    p_value = Z_LVAL_P(partition);
    if (p_value < -1)
    {
        zend_throw_exception(
            kafka_exception,
            "invalid partition passed to Kafka::setPartition, partition value should be >= 0 or Kafka::PARTION_RANDOM",
            0 TSRMLS_CC
        );
        return;
    }
    p_value = p_value == -1 ? PHP_KAFKA_PARTITION_RANDOM : p_value;
    if (!mode)
    {
        connection->consumer_partition = p_value;
        connection->producer_partition = p_value;
        kafka_set_partition(p_value);
    }
    else
    {
        if (Z_LVAL_P(mode) != PHP_KAFKA_MODE_CONSUMER)
            connection->producer_partition = p_value;
        else
            connection->consumer_partition = p_value;
    }
    //return $this
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::setPartition */

/* {{{ proto int Kafka::getPartition( int $mode )
    Get partition for connection (consumer/producer)
*/
PHP_METHOD(Kafka, getPartition)
{
    zval *obj = getThis(),
        *arg = NULL;
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &arg) == FAILURE)
        return;
    if (Z_TYPE_P(arg) != IS_LONG || (Z_LVAL_P(arg) != PHP_KAFKA_MODE_CONSUMER && Z_LVAL_P(arg) != PHP_KAFKA_MODE_PRODUCER))
    {
        zend_throw_exception(kafka_exception, "Invalid argument passed to Kafka::getPartition, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    if (Z_LVAL_P(arg) == PHP_KAFKA_MODE_CONSUMER)
        RETURN_LONG(connection->consumer_partition);
    RETURN_LONG(connection->producer_partition);
}
/* }}} end proto Kafka::getPartition */

/* {{{ proto array Kafka::getTopics( void )
    Get all existing topics
*/
PHP_METHOD(Kafka, getTopics)
{
    zval *obj = getThis();
    GET_KAFKA_CONNECTION(connection, obj);
    if (connection->brokers == NULL && connection->consumer == NULL)
    {
        zend_throw_exception(kafka_exception, "No brokers to get topics from", 0 TSRMLS_CC);
        return;
    }
    if (connection->consumer == NULL)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_CONSUMER;
        config.log_level = connection->log_level;
        config.queue_buffer = connection->queue_buffer;
        config.compression = NULL;
        connection->consumer = kafka_get_connection(config, connection->brokers);
        if (connection->consumer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_CONSUMER;
    }
    array_init(return_value);
    kafka_get_topics(connection->consumer, return_value);
}
/* }}} end Kafka::getTopics */

/* {{{ proto Kafka Kafka::setBrokers ( string $brokers [, array $options = null ] )
    Set brokers on-the-fly
*/
PHP_METHOD(Kafka, setBrokers)
{
    zval *arr = NULL,
        *obj = getThis();
    char *brokers;
    int brokers_len;
    GET_KAFKA_CONNECTION(connection, obj);

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|a!",
            &brokers, &brokers_len, &arr) == FAILURE) {
        return;
    }
    //if array is passed, parse it, return if an exception was thrown...
    if (arr && parse_options_array(arr, &connection))
        return;
    if (connection->consumer)
        kafka_destroy(connection->consumer, 1);
    if (connection->producer)
        kafka_destroy(connection->producer, 1);
    //free previous brokers value, if any
    if (connection->brokers)
        efree(connection->brokers);
    if (connection->compression)
        efree(connection->compression);
    //set brokers
    connection->brokers = estrdup(
        brokers
    );
    //reinit to NULL
    connection->producer = connection->consumer = NULL;
    connection->compression = NULL;
    //restore partitions back to random...
    connection->consumer_partition = connection->producer_partition = PHP_KAFKA_PARTITION_RANDOM;
    //set brokers member to correct value
    //we can ditch this call, I think...
    kafka_connect(
        connection->brokers
    );
    //return
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end Kafka::setBrokers */

/* proto Kafka Kafka::setOptions( array $options )
 * Set connection options on the "fly"
 */
PHP_METHOD(Kafka, setOptions)
{
    zval *arr = NULL,
        *obj = getThis();
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a", &arr) == FAILURE)
    {
        return;
    }
    if (parse_options_array(arr, &connection))
        return;
    RETURN_ZVAL(obj, 1, 0);

}
/* end proto Kafka::setOptions */

/* {{{ proto array Kafka::getPartitionsForTopic( string $topic )
    Get an array of available partitions for a given topic
*/
PHP_METHOD(Kafka, getPartitionsForTopic)
{
    zval *obj = getThis();
    char *topic = NULL;
    int topic_len = 0;
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &topic, &topic_len) == FAILURE) {
        return;
    }
    if (!connection->consumer)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_CONSUMER;
        config.log_level = connection->log_level;
        config.queue_buffer = connection->queue_buffer;
        config.compression = NULL;
        connection->consumer = kafka_get_connection(config, connection->brokers);
        if (connection->consumer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_CONSUMER;
    }
    array_init(return_value);
    kafka_get_partitions(connection->consumer, return_value, topic);
}
/* }}} end Kafka::getPartitionsForTopic */

/* {{{ proto Kafka::getPartitionOffsets( string $topic )
 * Get an array containing all partitions and their respective first offsets
 */
PHP_METHOD(Kafka, getPartitionOffsets)
{
    char *topic = NULL;
    int topic_len = 0,
        kafka_r;
    long *offsets = NULL,
        i;
    kafka_connection *connection = (kafka_connection *) zend_object_store_get_object(
        getThis() TSRMLS_CC
    );

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &topic, &topic_len) == FAILURE) {
        return;
    }
    if (!connection->consumer)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_CONSUMER;
        config.log_level = connection->log_level;
        config.queue_buffer = connection->queue_buffer;
        config.compression = NULL;
        connection->consumer = kafka_get_connection(config, connection->brokers);
        if (connection->consumer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_CONSUMER;
    }
    kafka_r = kafka_partition_offsets(
        connection->consumer,
        &offsets,
        topic
    );
    if (kafka_r < 1) {
        char *msg = NULL;
        if (kafka_r)
            msg = kafka_r == -2 ? "No kafka connection" : "Allocation error";
        else
            msg = "Failed to get metadata for topic";
        zend_throw_exception(
            kafka_exception,
            msg,
            0 TSRMLS_CC
        );
        return;
    }
    array_init(return_value);
    for (i=0;i<kafka_r;++i) {
        add_index_long(return_value,i, offsets[i]);
    }
    free(offsets);//kafka allocates this bit, free outside of zend
} /* }}} end Kafka::getPartitionOffsets */

/* {{{ proto bool Kafka::disconnect( [int $mode] );
   if No $mode argument is passed, all connections will be closed
    Disconnects kafka, returns false if disconnect failed
*/
PHP_METHOD(Kafka, disconnect)
{
    zval *obj = getThis(),
        *mode = NULL;
    long type = -1;
    GET_KAFKA_CONNECTION(connection, obj);
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z!",
            &mode) == FAILURE) {
        return;
    }
    if (mode)
    {//mode was given
        if (Z_TYPE_P(mode) == IS_LONG)
            type = Z_LVAL_P(mode);
        if (type != PHP_KAFKA_MODE_CONSUMER && type != PHP_KAFKA_MODE_PRODUCER)
        {
            zend_throw_exception(
                kafka_exception,
                "invalid argument passed to Kafka::disconnect, use Kafka::MODE_* constants",
                0 TSRMLS_CC
            );
            return;
        }
        if (type == PHP_KAFKA_MODE_CONSUMER)
        {//disconnect consumer
            if (connection->consumer)
                kafka_destroy(connection->consumer, 1);
            connection->consumer = NULL;
        }
        else
        {
            if (connection->producer)
                kafka_destroy(connection->producer, 1);
            connection->producer = NULL;
        }
        RETURN_TRUE;
    }
    if (connection->consumer)
        kafka_destroy(connection->consumer, 1);
    if (connection->producer)
        kafka_destroy(connection->producer, 1);
    connection->producer = connection->consumer = NULL;
    connection->consumer_partition = connection->producer_partition = PHP_KAFKA_PARTITION_RANDOM;
    RETURN_TRUE;
}
/* }}} end Kafka::disconnect */

/* {{{ proto Kafka Kafka::produce( string $topic, string $message);
    Produce a message, returns instance
    or throws KafkaException in case something went wrong
*/
PHP_METHOD(Kafka, produce)
{
    zval *object = getThis();
    GET_KAFKA_CONNECTION(connection, object);
    char *topic;
    char *msg;
    long reporting = connection->delivery_confirm_mode;
    int topic_len,
        msg_len,
        status = 0;


    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss",
            &topic, &topic_len,
            &msg, &msg_len) == FAILURE) {
        return;
    }
    if (!connection->producer)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_PRODUCER;
        config.log_level = connection->log_level;
        config.reporting = connection->delivery_confirm_mode;
        config.retry_count = connection->retry_count;
        config.retry_interval = connection->retry_interval;
        config.compression = connection->compression;
        connection->producer = kafka_get_connection(config, connection->brokers);
        if (connection->producer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_PRODUCER;
    }
    //this does nothing at this stage...
    kafka_set_partition(
        (int) connection->producer_partition
    );
    if (connection->delivery_confirm_mode == PHP_KAFKA_CONFIRM_EXTENDED)
        status = kafka_produce_report(connection->producer, topic, msg, msg_len);
    else
        status = kafka_produce(connection->producer, topic, msg, msg_len, connection->delivery_confirm_mode);
    switch (status)
    {
        case -1:
            zend_throw_exception(kafka_exception, "Failed to produce message", 0 TSRMLS_CC);
            return;
        case -2:
            zend_throw_exception(kafka_exception, "Connection failure, cannot produce message", 0 TSRMLS_CC);
            return;
    }
    RETURN_ZVAL(object, 1, 0);
}
/* }}} end Kafka::produce */

/* {{{ proto Kafka Kafka::produceBatch( string $topic, array $messages [, int $batchSize = 50 ]);
    Produce a batch of messages, returns instance
    or throws exceptions in case of error
*/
PHP_METHOD(Kafka, produceBatch)
{
    zval *arr,
         *object = getThis(),
         **entry;
    GET_KAFKA_CONNECTION(connection, object);
    char *topic;
    char *msg;
    char **msg_batch = NULL;
    int *msg_batch_len = NULL;
    long reporting = connection->delivery_confirm_mode,
        batch_size = connection->produce_batch_size;
    int topic_len,
        msg_len,
        current_idx = 0,
        status = 0;
    HashPosition pos;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sa|l",
            &topic, &topic_len,
            &arr, &batch_size) == FAILURE) {
        return;
    }
    if (batch_size < 1)
    {
        zend_throw_exception(
            kafka_exception,
            "Kafka::produceBatch requires batchSize to be > 1",
            0 TSRMLS_CC
        );
        return;
    }
    //get producer up and running
    if (!connection->producer)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_PRODUCER;
        config.log_level = connection->log_level;
        config.reporting = connection->delivery_confirm_mode;
        config.retry_count = connection->retry_count;
        config.compression = connection->compression;
        config.retry_interval = connection->retry_interval;
        connection->producer = kafka_get_connection(config, connection->brokers);
        if (connection->producer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_PRODUCER;
    }
    //this does nothing at this stage...
    kafka_set_partition(
        (int) connection->producer_partition
    );
    //iterate array of messages, start producing them
    //todo: change individual produce calls to a more performant
    //produce queue...
    zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(arr), &pos);
    msg_batch_len = ecalloc(batch_size, sizeof *msg_batch_len);
    msg_batch = emalloc(batch_size * sizeof *msg_batch);
    if (msg_batch == NULL || msg_batch_len == NULL)
    {//make sure we managed to allocate enough memory
        //this is ugly, we should trigger fatal error here...
        if (msg_batch_len)
            efree(msg_batch_len);
        if (msg_batch)
            efree(msg_batch);
        zend_error(
            E_ERROR,
            "Failed to allocate batch memroy"
        );
        return;
    }
    while (zend_hash_get_current_data_ex(Z_ARRVAL_P(arr), (void **)&entry, &pos) == SUCCESS)
    {
        if (Z_TYPE_PP(entry) == IS_STRING)
        {
            msg_batch[current_idx] = Z_STRVAL_PP(entry);
            msg_batch_len[current_idx] = Z_STRLEN_PP(entry);
            ++current_idx;
            if (current_idx == batch_size)
            {
                status = kafka_produce_batch(connection->producer, topic, msg_batch, msg_batch_len, current_idx, connection->delivery_confirm_mode);
                if (status)
                {
                    if (status < 0)
                        zend_throw_exception(kafka_exception, "Failed to produce messages", 0 TSRMLS_CC);
                    else if (status > 0)
                    {
                        char err_msg[200];
                        snprintf(err_msg, 200, "Produced messages with %d errors", status);
                        zend_throw_exception(kafka_exception, err_msg, 0 TSRMLS_CC);
                    }
                    efree(msg_batch_len);
                    efree(msg_batch);
                    return;
                }
                current_idx = 0;//reset batch counter
            }
        }
        zend_hash_move_forward_ex(Z_ARRVAL_P(arr), &pos);
    }
    if (current_idx)
    {//we still have some messages to produce...
        status = kafka_produce_batch(connection->producer, topic, msg_batch, msg_batch_len, current_idx, connection->delivery_confirm_mode);
        if (status)
        {
            if (status < 0)
                zend_throw_exception(kafka_exception, "Failed to produce messages", 0 TSRMLS_CC);
            else if (status > 0)
            {
                char err_msg[200];
                snprintf(err_msg, 200, "Produced messages with %d errors", status);
                zend_throw_exception(kafka_exception, err_msg, 0 TSRMLS_CC);
            }
            efree(msg_batch_len);
            efree(msg_batch);
            return;
        }
    }
    efree(msg_batch_len);
    efree(msg_batch);
    RETURN_ZVAL(object, 1, 0);
}
/* end proto Kafka::produceBatch */

/* {{{ proto array Kafka::consume( string $topic, [ string $offset = 0 [, mixed $length = 1] ] );
    Consumes 1 or more ($length) messages from the $offset (default 0)
*/
PHP_METHOD(Kafka, consume)
{
    zval *object = getThis();
    GET_KAFKA_CONNECTION(connection, object);
    char *topic;
    int topic_len;
    char *offset;
    int offset_len, status = 0;
    long count = 0;
    zval *item_count = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss|z",
            &topic, &topic_len,
            &offset, &offset_len,
            &item_count) == FAILURE) {
        return;
    }
    if (item_count == NULL || Z_TYPE_P(item_count) == IS_NULL)
    {//default
        count = 1;
    }
    else
    {
        if (Z_TYPE_P(item_count) == IS_STRING && strcmp(Z_STRVAL_P(item_count), PHP_KAFKA_OFFSET_END) == 0) {
            count = -1;
        } else if (Z_TYPE_P(item_count) == IS_LONG) {
            count = Z_LVAL_P(item_count);
        } else {

            zend_throw_exception(
                kafka_exception,
                "Invalid messageCount value passed to Kafka::consume, should be int or OFFSET constant",
                0 TSRMLS_CC
            );
        }
    }
    if (count < -1 || count == 0)
    {
        zend_throw_exception(
            kafka_exception,
            "Invalid messageCount value passed to Kafka::consume",
            0 TSRMLS_CC
        );
    }
    if (!connection->consumer)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_CONSUMER;
        config.log_level = connection->log_level;
        config.queue_buffer = connection->queue_buffer;
        config.compression = NULL;
        connection->consumer = kafka_get_connection(config, connection->brokers);
        if (connection->consumer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_CONSUMER;
    }
    array_init(return_value);
    if (connection->consumer_partition == PHP_KAFKA_PARTITION_RANDOM)
    {
        kafka_consume_all(
            connection->consumer,
            return_value,
            topic,
            offset,
            count
        );
    }
    else
    {
        status = kafka_consume(
            connection->consumer,
            return_value,
            topic,
            offset,
            count,
            connection->consumer_partition
        );
        if (status)
        {
            switch (status)
            {
                case -1:
                    zend_throw_exception(
                        kafka_exception,
                        "Invalid offset passed, use Kafka::OFFSET_* constants, or positive integer!",
                        0 TSRMLS_CC
                    );
                    return;
                case -2:
                    zend_throw_exception(
                        kafka_exception,
                        "No kafka connection available",
                        0 TSRMLS_CC
                    );
                    return;
                case -3:
                    zend_throw_exception(
                        kafka_exception,
                        "Unable to access topic",
                        0 TSRMLS_CC
                    );
                    return;
                case -4:
                default:
                    zend_throw_exception(
                        kafka_exception,
                        "Consuming from topic failed",
                        0 TSRMLS_CC
                    );
                    return;
            }
        }
    }
}
/* }}} end Kafka::consume */

/* {{{ proto array Kafka::consumeBatch(string $topic, string $offset [, int $batchSize = 50 ])
 * Consume in batches (should be more performant when consuming large number of messages
 */
PHP_METHOD(Kafka, consumeBatch)
{
    zval *object = getThis();
    GET_KAFKA_CONNECTION(connection, object);
    char *topic;
    int topic_len;
    char *offset;
    int offset_len, status = 0;
    long count = 0,
        batch_size = connection->consume_batch_size;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl",
            &topic, &topic_len,
            &offset, &offset_len,
            &batch_size) == FAILURE) {
        return;
    }
    if (connection->consumer_partition == PHP_KAFKA_PARTITION_RANDOM)
    {
        zend_throw_exception(kafka_exception, "Kafka::consumeBatch requires a partition to be set", 0 TSRMLS_CC);
        return;
    }
    if (batch_size < 1)
    {//default
        zend_throw_exception(
            kafka_exception,
            "Invalid batchSize passed to Kafka::consumeBatch, should be >= 1",
            0 TSRMLS_CC
        );
    }
    if (!connection->consumer)
    {
        kafka_connection_params config;
        config.type = RD_KAFKA_CONSUMER;
        config.log_level = connection->log_level;
        config.queue_buffer = connection->queue_buffer;
        config.compression = NULL;
        connection->consumer = kafka_get_connection(config, connection->brokers);
        if (connection->consumer == NULL)
        {
            zend_throw_exception(kafka_exception, "Failed to connect to kafka", 0 TSRMLS_CC);
            return;
        }
        connection->rk_type = RD_KAFKA_CONSUMER;
    }
    array_init(return_value);
    //int kafka_consume_batch(rd_kafka_t *r, zval* return_value, char *topic, char *offset, long item_count, int partition)
    status = kafka_consume_batch(
        connection->consumer,
        return_value,
        topic,
        offset,
        count,
        connection->consumer_partition
    );
    if (status)
    {
        switch (status)
        {
            case -1:
                zend_throw_exception(
                    kafka_exception,
                    "Invalid offset passed, use Kafka::OFFSET_* constants, or positive integer!",
                    0 TSRMLS_CC
                );
                return;
            case -2:
                zend_throw_exception(
                    kafka_exception,
                    "No kafka connection available",
                    0 TSRMLS_CC
                );
                return;
            case -3:
                zend_throw_exception(
                    kafka_exception,
                    "Unable to access topic",
                    0 TSRMLS_CC
                );
                return;
            case -5:
                zend_throw_exception(kafka_exception, "Memory allocation failed", 0 TSRMLS_CC);
                return;
            case -4:
            default:
                zend_throw_exception(
                    kafka_exception,
                    "Consuming from topic failed",
                    0 TSRMLS_CC
                );
                return;
        }
    }
}
/* }}} end proto Kafka::consumeBatch */

/* {{{ proto KafkaTopic Kafka::getTopic( string $topicName, int $mode)
 * Return instance of KafkaTopic to use for producing or consuming
 */
PHP_METHOD(Kafka, getTopic)
{
    zval *obj = getThis();
    GET_KAFKA_CONNECTION(connection, obj);
    char *topic_name;
    int topic_name_len;
    long mode = 0;//PHP_KAFKA_MODE_CONSUMER default?
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl", &topic_name, &topic_name_len, &mode) != SUCCESS)
        return;//fatal
    //topic_name argument validation is handled by KafkaTopic constructor
    if (mode != PHP_KAFKA_MODE_CONSUMER && mode != PHP_KAFKA_MODE_PRODUCER)
    {
        zend_throw_exception(kafka_exception, "Invalid mode, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    if (mode == PHP_KAFKA_MODE_CONSUMER)
    {
        if (connection->consumer == NULL)
        {
            kafka_connection_params config;
            config.type = RD_KAFKA_CONSUMER;
            config.log_level = connection->log_level;
            config.queue_buffer = connection->queue_buffer;
            config.compression = NULL;
            connection->consumer = kafka_get_connection(config, connection->brokers);
            if (connection->consumer == NULL)
            {
                zend_throw_exception(kafka_exception, "Failed to establish consumer connection to kafka", 0 TSRMLS_CC);
                return;
            }
        }
    }
    else // if (mode == PHP_KAFKA_MODE_PRODUCER)
    {//assume
        if (connection->producer == NULL)
        {
            kafka_connection_params config;
            config.type = RD_KAFKA_PRODUCER;
            config.log_level = connection->log_level;
            config.reporting = connection->delivery_confirm_mode;
            config.retry_count = connection->retry_count;
            config.compression = connection->compression;
            config.retry_interval = connection->retry_interval;
            connection->producer = kafka_get_connection(config, connection->brokers);
            if (connection->producer == NULL)
            {
                zend_throw_exception(kafka_exception, "Failed to establish producer connection to kafka", 0 TSRMLS_CC);
                return;
            }
        }
    }
    //call the constructor, pass Kafka instance, the topic name and the mode value
    //technically, though, there's no need to do this
    //we initialized KafkaTopic here, so we can set the connection, and topic name
    //changed to initializing everything here.
    //CALL_METHOD3(KafkaTopic, __construct, return_value, return_value, obj, &z_topic_name, &z_mode);

    object_init_ex(return_value, kafka_topic_ce);
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(return_value TSRMLS_CC);
    if (mode == PHP_KAFKA_MODE_CONSUMER)
    {
        topic->conn = connection->consumer;
        connection->consumer = NULL;
        topic->rk_type =RD_KAFKA_CONSUMER;
    }
    else
    {
        topic->conn = connection->producer;
        connection->producer = NULL;
        topic->rk_type =RD_KAFKA_PRODUCER;
    }
    topic->topic_name = estrdup(topic_name);
    if (0 != init_kafka_topic_handle(topic->conn, topic_name, topic->rk_type, &topic->topic, &topic->config))
    {
        zend_throw_exception(
            kafka_exception,
            "Failed to create topic handle: make sure the connection is valid, and the topic exists",
            0 TSRMLS_CC
        );
        return;
    }

}
/* }}} end proto Kafka::getTopic */

/* {{{ proto array KafkaTopic::__construct(Kafka $connection, int $mode )
 * Consume in batches (should be more performant when consuming large number of messages
 */
PHP_METHOD(KafkaTopic, __construct)
{
    zval *object = getThis(),
         *kafka;
    char *topic_name;
    int topic_name_len;
    long mode = 0;//PHP_KAFKA_MODE_CONSUMER default?
    kafka_connection *connection = NULL;
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(object TSRMLS_CC);
    //
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "Osl", &kafka, kafka_ce, &topic_name, &topic_name_len, &mode) != SUCCESS)
        return;//fatal
    connection = (kafka_connection *) zend_object_store_get_object(kafka TSRMLS_CC);
    if (mode != PHP_KAFKA_MODE_CONSUMER && mode != PHP_KAFKA_MODE_PRODUCER) {
        zend_throw_exception(kafka_exception, "Invalid mode, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    if (topic_name_len == 0) {
        zend_throw_exception(kafka_exception, "No topic name given", 0 TSRMLS_CC);
        return;
    }
    //move connection to this instance
    if (mode == PHP_KAFKA_MODE_CONSUMER) {
        if (connection->consumer == NULL)
        {
            zend_throw_exception(
                kafka_exception,
                "Kafka instance has no consumer connection ready",
                0 TSRMLS_CC
            );
            return;
        }
        topic->conn = connection->consumer;
        connection->consumer = NULL;
        topic->rk_type =RD_KAFKA_CONSUMER;
    } else {
        if (connection->producer == NULL)
        {
            zend_throw_exception(
                kafka_exception,
                "Kafka instance has no producer connection ready",
                0 TSRMLS_CC
            );
            return;
        }
        topic->conn = connection->producer;
        connection->producer = NULL;
        topic->rk_type =RD_KAFKA_PRODUCER;
    }
    topic->topic_name = estrdup(topic_name);
    if (0 != init_kafka_topic_handle(topic->conn, topic_name, topic->rk_type, &topic->topic, &topic->config))
    {
        zend_throw_exception(
            kafka_exception,
            "Failed to create topic handle: make sure the connection is valid, and the topic exists",
            0 TSRMLS_CC
        );
        return;
    }
}

/* {{{ proto string KafkaTopic::getName( void )
 * Return current topic name
 */
PHP_METHOD(KafkaTopic, getName)
{
    zval *obj = getThis();
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(obj TSRMLS_CC);
    RETURN_STRING(topic->topic_name, 1);
}
/* }}} end proto KafkaTopic::getName */

/* {{{ proto int KafkaTopic::getTopicCount( void )
 * Return the current topic's partition count
 */
PHP_METHOD(KafkaTopic, getPartitionCount)
{
    zval *obj = getThis();
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(obj TSRMLS_CC);
    if (topic->meta == NULL)
    {
        topic->meta = get_topic_meta(topic->conn, topic->topic);
        if (topic->meta == NULL)
        {
            zend_throw_exception(
                kafka_exception,
                "Failed to fetch metadata for topic",
                0 TSRMLS_CC
            );
        }
    }
    RETURN_LONG(topic->meta->topics->partition_cnt);
}
/* }}} end proto KafkaTopic::getTopicCount */

/* {{{ proto KafkaTopic KafkaTopic::produce( string $message )
 * Produce a single message (real-time/blocking)
 */
PHP_METHOD(KafkaTopic, produce)
{
    zval *obj = getThis();
    char *message = NULL;
    int status, message_len = 0;
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(obj TSRMLS_CC);
    if (topic->rk_type != RD_KAFKA_PRODUCER)
    {
        zend_throw_exception(
            kafka_exception,
            "produce-calls require a topic in produce-mode",
            0 TSRMLS_CC
        );
        return;
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &message, &message_len) != SUCCESS)
        return;//fatal

    //always reports => blocking call
    status = kafka_topic_produce(topic->conn, topic->topic, message, message_len);

    switch (status)
    {
        case -1:
            zend_throw_exception(kafka_exception, "Failed to produce message", 0 TSRMLS_CC);
            return;
        case -2:
            zend_throw_exception(kafka_exception, "Connection failure, cannot produce message", 0 TSRMLS_CC);
            return;
    }
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto KafkaTopic::produce */

/* {{{ proto KafkaTopic KafkaTopic::produceBatch( array $messages [, int $batchSize = 0 [, bool $blocking = false] ] )
 * Produce messages in batches of $batchSize, if batch can't be allocated, there's a fallback to sequential (blocking) produce calls
 * if $batchSize is 0 (default) the entire array will be produced as a single batch
 */
PHP_METHOD(KafkaTopic, produceBatch)
{
    zval *obj = getThis(),
         *arr,
         **entry;
    char **msg_batch = NULL;
    int *msg_batch_len = NULL;
    int current_idx = 0,
        batch_size = 0,
        status = 0;
    HashPosition pos;
    zend_bool blocking = 0;
    //make sure the connection is producer
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(obj TSRMLS_CC);
    if (topic->rk_type != RD_KAFKA_PRODUCER)
    {
        zend_throw_exception(
            kafka_exception,
            "produce-calls require a topic in produce-mode",
            0 TSRMLS_CC
        );
        return;
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a|lb", &arr, &batch_size, &blocking) == FAILURE)
        return;

    //default batch size == the entire array
    if (batch_size == 0)
        batch_size = zend_hash_num_elements(Z_ARRVAL_P(arr));
    if (batch_size < 1)
    {//valid arguments only
        zend_throw_exception(
            kafka_exception,
            "Kafka::produceBatch requires batchSize to be > 0",
            0 TSRMLS_CC
        );
        return;
    }

    //iterate array of messages, start producing them
    //todo: change individual produce calls to a more performant
    //produce queue...
    zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(arr), &pos);
    msg_batch_len = ecalloc(batch_size, sizeof *msg_batch_len);
    msg_batch = emalloc(batch_size * sizeof *msg_batch);
    if (msg_batch == NULL || msg_batch_len == NULL)
    {//make sure we managed to allocate enough memory
        //this is ugly, we should trigger fatal error here...
        if (msg_batch_len)
            efree(msg_batch_len);
        if (msg_batch)
            efree(msg_batch);
        zend_error(
            E_ERROR,
            "Failed to allocate batch memroy"
        );
        return;
    }
    while (zend_hash_get_current_data_ex(Z_ARRVAL_P(arr), (void **)&entry, &pos) == SUCCESS)
    {
        if (Z_TYPE_PP(entry) == IS_STRING)
        {
            msg_batch[current_idx] = Z_STRVAL_PP(entry);
            msg_batch_len[current_idx] = Z_STRLEN_PP(entry);
            ++current_idx;
            if (current_idx == batch_size)
            {
                //default batch produce => sync...
                status = kafka_topic_produce_batch(topic->conn, topic->topic, msg_batch, msg_batch_len, current_idx, blocking);
                if (status)
                {
                    if (status < 0)
                        zend_throw_exception(kafka_exception, "Failed to produce messages", 0 TSRMLS_CC);
                    else if (status > 0)
                    {
                        char err_msg[200];
                        snprintf(err_msg, 200, "Produced messages with %d errors", status);
                        zend_throw_exception(kafka_exception, err_msg, 0 TSRMLS_CC);
                    }
                    efree(msg_batch_len);
                    efree(msg_batch);
                    return;
                }
                current_idx = 0;//reset batch counter
            }
        }
        zend_hash_move_forward_ex(Z_ARRVAL_P(arr), &pos);
    }
    if (current_idx)
    {//we still have some messages to produce...
        status = kafka_produce_batch(topic->conn, topic->topic_name, msg_batch, msg_batch_len, current_idx, 1);
        if (status)
        {
            if (status < 0)
                zend_throw_exception(kafka_exception, "Failed to produce messages", 0 TSRMLS_CC);
            else if (status > 0)
            {
                char err_msg[200];
                snprintf(err_msg, 200, "Produced messages with %d errors", status);
                zend_throw_exception(kafka_exception, err_msg, 0 TSRMLS_CC);
            }
            efree(msg_batch_len);
            efree(msg_batch);
            return;
        }
    }
    efree(msg_batch_len);
    efree(msg_batch);
    RETURN_ZVAL(obj, 1, 0);

}
/* }}} end proto KafkaTopic::produceBatch */

/* {{{ proto array KafkaTopic::consume( [ int $messageCount = 1 [, mixed $offset =  Kafka::OFFSET_STORED]] )
 * Consume X messages starting at $offset (default last), default $messageCount is 1
 */
PHP_METHOD(KafkaTopic, consume)
{
    zval *obj = getThis();
    char *offset = NULL;
    int status, offset_len = 0;
    long item_count = 1;
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(obj TSRMLS_CC);
    if (topic->rk_type != RD_KAFKA_CONSUMER)
    {
        zend_throw_exception(
            kafka_exception,
            "consume-calls require a topic in consumer-mode",
            0 TSRMLS_CC
        );
        return;
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|ls", &item_count, &offset, &offset_len) != SUCCESS)
        return;//fatal
    if (topic->meta == NULL)
    {
        topic->meta = get_topic_meta(topic->conn, topic->topic);
        if (topic->meta == NULL)
        {
            zend_throw_exception(
                kafka_exception,
                "Failed to fetch metadata for topic",
                0 TSRMLS_CC
            );
        }
    }
    if (!offset_len)
        offset = PHP_KAFKA_OFFSET_STORED;
    array_init(return_value);
    status = kafka_topic_consume(topic->conn, topic->topic, topic->meta, return_value, offset, item_count);
    if (status)
    {
        zend_throw_exception(kafka_exception, "Failed to consume messages", status TSRMLS_CC);
    }
}
/* }}} end proto KafkaTopic::consume */

/* {{{ proto array KafkaTopic::consumeBatch( [ int $batchSize = -1 [, mixed $offset =  Kafka::OFFSET_STORED ] ])
 * Note that the array returned by this method CAN change over time, still... we need a separate class to cancel/destroy the consume queue
 * And track its process
 */
PHP_METHOD(KafkaTopic, consumeBatch)
{
    zval *obj = getThis();
    char *offset = NULL;
    int status, offset_len = 0;
    long item_count = -1;
    kafka_topic *topic = (kafka_topic *) zend_object_store_get_object(obj TSRMLS_CC);
    if (topic->rk_type != RD_KAFKA_CONSUMER)
    {
        zend_throw_exception(
            kafka_exception,
            "consume-calls require a topic in consumer-mode",
            0 TSRMLS_CC
        );
        return;
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|ls", &item_count, &offset, &offset_len) != SUCCESS)
        return;//fatal
    if (item_count < -1 || item_count == 0)
    {
        zend_throw_exception(
            kafka_exception,
            "Invalid value for batchSize argument (-1 or positive int expected)",
            0 TSRMLS_CC
        );
        return;
    }
    if (topic->meta == NULL)
    {
        topic->meta = get_topic_meta(topic->conn, topic->topic);
        if (topic->meta == NULL)
        {
            zend_throw_exception(
                kafka_exception,
                "Failed to fetch metadata for topic",
                0 TSRMLS_CC
            );
        }
    }
    if (!offset_len)
        offset = PHP_KAFKA_OFFSET_STORED;
    array_init(return_value);
    status = kafka_topic_consume_batch(topic->conn, topic->topic, topic->meta, return_value, offset, item_count);
    if (status)
    {
        zend_throw_exception(kafka_exception, "Failed to consume messages", status TSRMLS_CC);
    }
}
/* }}} end proto KafkaTopic::consumeBatch */

/* {{{ proto int KafkaQueue::getStatus( void )
 * returns the status in KafkaQueue::* status constant form
 */
PHP_METHOD(KafkaQueue, getStatus)
{
    zval *obj = getThis();
    kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(obj TSRMLS_CC);
    RETURN_LONG(queue->status);
}

/* {{{ proto array KafkaQueue::getMessages( [ bool $andStop = false ] )
 * Get messages consumed, and stop if desired
 */
PHP_METHOD(KafkaQueue, getMessages)
{
    zval *obj = getThis();
    zend_bool and_stop = 0;
    int pauzed = 0;
    kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(obj TSRMLS_CC);
    if (queue->status != PHP_KAFKA_QUEUE_IDLE)
    {
        if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &and_stop) != SUCCESS)
            return;//fatal
        ZVAL_COPY_VALUE(return_value, queue->params.msg_arr);
        if (and_stop)
        {
            queue->status = PHP_KAFKA_QUEUE_DONE;
            //@todo stop queue
        }
        else if (queue->status == PHP_KAFKA_QUEUE_CONSUMING)
        {
            pauzed = 1;
            //we're currently consuming, pauze while copying
            queue->params.consume_pauze = 1;
        }
        zval_copy_ctor(return_value);
        if (pauzed)
        {
            queue->params.consume_pauze = 0;
        }
    }
    else
    {
        array_init(return_value);//create new, empty, array
    }
}
/* }}} end proto KafkaQueue::getMessages */

/* {{{ proto KafkaTopic KafkaTopic::stop( void )
 * Stop consuming
 */
PHP_METHOD(KafkaQueue, stop)
{
    zval *obj = getThis();
    kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(obj TSRMLS_CC);
    if (queue->status != PHP_KAFKA_QUEUE_IDLE && queue->status != PHP_KAFKA_QUEUE_DONE)
    {
        //@todo implement stop consume
    }
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto KafkaQueue::stop */

/* {{{ proto KafkaTopic KafkaTopic::pauze( void )
 * pauze consuming
 */
PHP_METHOD(KafkaQueue, pauze)
{
    zval *obj = getThis();
    kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(obj TSRMLS_CC);
    if (queue->status == PHP_KAFKA_QUEUE_CONSUMING)
    {
        queue->status = PHP_KAFKA_QUEUE_PAUZED;
        queue->params.consume_pauze = 1;
    }
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto KafkaTopic::pauze */

/* {{{ proto KafkaTopic KafkaTopic::resume( void )
 * resume consuming
 */
PHP_METHOD(KafkaQueue, resume)
{
    zval *obj = getThis();
    kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(obj TSRMLS_CC);
    if (queue->status != PHP_KAFKA_QUEUE_PAUZED)
    {
        zend_throw_exception(
            kafka_exception,
            "Cannot resume consuming on a queue that is not pauzed",
            0 TSRMLS_CC
        );
    }
    queue->params.consume_pauze = 0;
    queue->status = PHP_KAFKA_QUEUE_CONSUMING;
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto KafkaTopic::resume */

/* {{{ proto void KafkaTopic::__destruct( void )
 * destructor (do we need this?)
 */
PHP_METHOD(KafkaQueue, __destruct)
{
    zval *obj = getThis();
    kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(obj TSRMLS_CC);
    Z_DELREF_P(queue->params.msg_arr);
}
