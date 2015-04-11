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

/* {{{ arginfo */
ZEND_BEGIN_ARG_INFO(arginf_kafka__constr, 0)
    ZEND_ARG_INFO(0, brokers)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_set_partition, 0, 0, 1)
    ZEND_ARG_INFO(0, partition)
    ZEND_ARG_INFO(0, mode)
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

ZEND_BEGIN_ARG_INFO(arginf_kafka_produce_batch, 0)
    ZEND_ARG_INFO(0, topic)
    ZEND_ARG_INFO(0, messages)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_consume, 0, 0, 2)
    ZEND_ARG_INFO(0, topic)
    ZEND_ARG_INFO(0, offset)
    ZEND_ARG_INFO(0, messageCount)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_is_conn, 0, 0, 0)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO(arginf_kafka_void, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_disconnect, 0, 0, 0)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

/* }}} end arginfo */

/* decalre the class entry */
zend_class_entry *kafka_ce;
zend_class_entry *kafka_exception;

/* the method table */
/* each method can have its own parameters and visibility */
static zend_function_entry kafka_functions[] = {
    PHP_ME(Kafka, __construct, arginf_kafka__constr, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, __destruct, arginf_kafka_void, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, set_partition, arginf_kafka_set_partition, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
    PHP_ME(Kafka, setPartition, arginf_kafka_set_partition, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartition, arginf_kafka_set_get_partition, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setLogLevel, arginf_kafka_set_log_level, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartitionsForTopic, arginf_kafka_get_partitions_for_topic, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartitionOffsets, arginf_kafka_get_partitions_for_topic, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setBrokers, arginf_kafka__constr, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getTopics, arginf_kafka_void, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, disconnect, arginf_kafka_disconnect, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, isConnected, arginf_kafka_is_conn, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, produce, arginf_kafka_produce, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, produceBatch, arginf_kafka_produce_batch, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, consume, arginf_kafka_consume, ZEND_ACC_PUBLIC)
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
    NULL, /* Module information */
    PHP_KAFKA_VERSION, /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

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

#define GET_KAFKA_CONNECTION(varname, thisObj) \
    kafka_connection *varname = (kafka_connection *) zend_object_store_get_object( \
        thisObj TSRMLS_CC \
    )


PHP_MINIT_FUNCTION(kafka)
{
    zend_class_entry ce,
            ce_ex;
    INIT_CLASS_ENTRY(ce, "Kafka", kafka_functions);
    kafka_ce = zend_register_internal_class(&ce TSRMLS_CC);
    INIT_CLASS_ENTRY(ce_ex, "KafkaException", NULL);
    kafka_exception = zend_register_internal_class_ex(
        &ce_ex,
        BASE_EXCEPTION,
        NULL TSRMLS_CC
    );
    //do not allow people to extend this class, make it final
    kafka_ce->create_object = create_kafka_connection;
    kafka_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, OFFSET_BEGIN, STRING);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, OFFSET_END, STRING);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, LOG_ON, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, LOG_OFF, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, MODE_CONSUMER, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, MODE_PRODUCER, LONG);
    REGISTER_KAFKA_CLASS_CONST(kafka_ce, PARTITION_RANDOM, LONG);
    return SUCCESS;
}
PHP_RSHUTDOWN_FUNCTION(kafka) { return SUCCESS; }
PHP_RINIT_FUNCTION(kafka) { return SUCCESS; }
PHP_MSHUTDOWN_FUNCTION(kafka) {
    return SUCCESS;
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

    zend_object_std_init(&intern->std, class_type TSRMLS_CC);
    //add properties table
#if PHP_VERSION_ID < 50399
    zend_hash_copy(
        interns->std.properties, &class_type->default_properties,
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
    retval.handlers = zend_get_std_object_handlers();

    return retval;
}

//clean current connections
void free_kafka_connection(void *object TSRMLS_DC)
{
    kafka_connection *connection = ((kafka_connection *) object);
    if (connection->brokers)
        efree(connection->brokers);
    if (connection->consumer != NULL)
        kafka_destroy(
            connection->consumer,
            1
        );
    if (connection->producer != NULL)
        kafka_destroy(
            connection->producer,
            1
        );
    efree(connection);
}

/** {{{ proto void DOMDocument::__construct( string $brokers );
    Constructor, expects a comma-separated list of brokers to connect to
*/
PHP_METHOD(Kafka, __construct)
{
    char *brokers = NULL;
    int brokers_len = 0;
    kafka_connection *connection = (kafka_connection *) zend_object_store_get_object(
        getThis() TSRMLS_CC
    );

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &brokers, &brokers_len) == FAILURE) {
        return;
    }
    connection->brokers = estrdup(brokers);
    kafka_connect(brokers);
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
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z", &mode) == FAILURE)
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
    kafka_connection *connection = (kafka_connection *) zend_object_store_get_object(
        getThis() TSRMLS_CC
    );
    if (connection->brokers)
        efree(connection->brokers);
    if (connection->consumer != NULL)
        kafka_destroy(
            connection->consumer,
            10
        );
    if (connection->producer != NULL)
        kafka_destroy(
            connection->producer,
            10
        );
    connection->producer    = NULL;
    connection->brokers     = NULL;
    connection->consumer    = NULL;
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
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &partition, &mode) == FAILURE)
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
    zval *log_level;
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
    kafka_set_log_level(Z_LVAL_P(log_level));
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::setLogLevel */

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
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &partition, &mode) == FAILURE)
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
    }
    if (connection->consumer == NULL)
    {
        connection->consumer = kafka_set_connection(
            RD_KAFKA_CONSUMER,
            connection->brokers
        );
        connection->rk_type = RD_KAFKA_CONSUMER;
    }
    array_init(return_value);
    kafka_get_topics(connection->consumer, return_value);
}
/* }}} end Kafka::getTopics */

/* {{{ proto Kafka Kafka::setBrokers ( string $brokers )
    Set brokers on-the-fly
*/
PHP_METHOD(Kafka, setBrokers)
{
    zval *brokers,
        *obj = getThis();
    GET_KAFKA_CONNECTION(connection, obj);

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z",
            &brokers) == FAILURE) {
        return;
    }
    if (Z_TYPE_P(brokers) != IS_STRING || Z_STRLEN_P(brokers) == 0) {
        zend_throw_exception(kafka_exception, "Kafka::setBrokers expects argument to be a non-empty string", 0 TSRMLS_CC);
        return;
    }
    if (connection->consumer)
        kafka_destroy(connection->consumer, 1);
    if (connection->producer)
        kafka_destroy(connection->producer, 1);
    //free previous brokers value, if any
    if (connection->brokers)
        efree(connection->brokers);
    //set brokers
    connection->brokers = estrdup(
        Z_STRVAL_P(brokers)
    );
    //reinit to NULL
    connection->producer = connection->consumer = NULL;
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
        connection->consumer = kafka_set_connection(RD_KAFKA_CONSUMER, connection->brokers);
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
        connection->consumer = kafka_set_connection(RD_KAFKA_CONSUMER, connection->brokers);
    }
    kafka_r = kafka_partition_offsets(
        connection->consumer,
        &offsets,
        topic
    );
    if (kafka_r < 1) {
        const char *msg = kafka_r == 1 ? "Failed to get metadata" : "unknown partition count (or mem-error)";
        zend_throw_exception(
            kafka_exception,
            msg,
            0 TSRMLS_CC
        );
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
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z",
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
        connection->producer = kafka_set_connection(RD_KAFKA_PRODUCER, connection->brokers);
        connection->rk_type = RD_KAFKA_PRODUCER;
    }
    //this does nothing at this stage...
    kafka_set_partition(
        (int) connection->producer_partition
    );
    status = kafka_produce(connection->producer, topic, msg, msg_len);
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

/* {{{ proto Kafka Kafka::produceBatch( string $topic, array $messages);
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
    int topic_len,
        msg_len,
        status = 0;
    HashPosition pos;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sa",
            &topic, &topic_len,
            &arr) == FAILURE) {
        return;
    }
    //get producer up and running
    if (!connection->producer)
    {
        connection->producer = kafka_set_connection(RD_KAFKA_PRODUCER, connection->brokers);
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
    while (zend_hash_get_current_data_ex(Z_ARRVAL_P(array), (void **)&entry, &pos) == SUCCESS)
    {
		if (Z_TYPE_PP(entry) == IS_STRING)
		{
            msg = Z_STRVAL_PP(entry);
            msg_len = Z_STRLEN_PP(entry);
            status = kafka_produce(
                connection->producer,
                topic,
                msg,
                msg_len
            );
            switch (status)
            {
                case -1:
                    zend_throw_exception(kafka_exception, "Failed to produce message", 0 TSRMLS_CC);
                    return;
                case -2:
                    zend_throw_exception(kafka_exception, "Connection failure, cannot produce message", 0 TSRMLS_CC);
                    return;
            }
		}
		zend_hash_move_forward_ex(Z_ARRVAL_P(arr), &pos);
	}
    /*
     * This bit is actually using the PHP7 array implementation
     * num_idx is zend_ulong and str_idx is zend_string
    ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL_P(arr), num_idx, str_idx, entry)
    {
        if (Z_TYPE_P(entry) == IS_STRING)
        {
            msg = Z_STRVAL_P(entry);
            msg_len = Z_STRLEN_P(entry);
            status = kafka_produce(
                connection->producer,
                topic,
                msg,
                msg_len
            );
            switch (status)
            {
                case -1:
                    zend_throw_exception(kafka_exception, "Failed to produce message", 0 TSRMLS_CC);
                    return;
                case -2:
                    zend_throw_exception(kafka_exception, "Connection failure, cannot produce message", 0 TSRMLS_CC);
                    return;
            }

        }
    } ZEND_HASH_FOREACH_END();
    */
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
        connection->consumer = kafka_set_connection(RD_KAFKA_CONSUMER, connection->brokers);
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
