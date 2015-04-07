/**
 *  Copyright 2013-2014 Patrick Reilly.
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
 */

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <php.h>
#include <php_kafka.h>
#include "kafka.h"
#include "zend_exceptions.h"

/* decalre the class entry */
zend_class_entry *kafka_ce;

/* the method table */
/* each method can have its own parameters and visibility */
static zend_function_entry kafka_functions[] = {
    PHP_ME(Kafka, __construct, NULL, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, __destruct, NULL, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, set_partition, NULL, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
    PHP_ME(Kafka, setPartition, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setLogLevel, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartitionsForTopic, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getPartitionOffsets, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setBrokers, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getTopics, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, disconnect, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, isConnected, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, produce, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, consume, NULL, ZEND_ACC_PUBLIC)
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

#ifndef BASE_EXCEPTION
#if (PHP_MAJOR_VERSION < 5) || ( ( PHP_MAJOR_VERSION == 5 ) && (PHP_MINOR_VERSION < 2) )
#define BASE_EXCEPTION zend_exception_get_default()
#else
#define BASE_EXCEPTION zend_exception_get_default(TSRMLS_C)
#endif
#endif

PHP_MINIT_FUNCTION(kafka)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "Kafka", kafka_functions);
    kafka_ce = zend_register_internal_class(&ce TSRMLS_CC);
    //do not allow people to extend this class, make it final
    kafka_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
    zend_declare_property_null(kafka_ce, "partition", sizeof("partition") -1, ZEND_ACC_PRIVATE TSRMLS_CC);
    REGISTER_KAFKA_CLASS_CONST_STRING(kafka_ce, "OFFSET_BEGIN", PHP_KAFKA_OFFSET_BEGIN);
    REGISTER_KAFKA_CLASS_CONST_STRING(kafka_ce, "OFFSET_END", PHP_KAFKA_OFFSET_END);
    REGISTER_KAFKA_CLASS_CONST_LONG(kafka_ce, "LOG_ON", PHP_KAFKA_LOGLEVEL_ON);
    REGISTER_KAFKA_CLASS_CONST_LONG(kafka_ce, "LOG_OFF", PHP_KAFKA_LOGLEVEL_OFF);
    return SUCCESS;
}
PHP_RSHUTDOWN_FUNCTION(kafka) { return SUCCESS; }
PHP_RINIT_FUNCTION(kafka) { return SUCCESS; }
PHP_MSHUTDOWN_FUNCTION(kafka) {
    kafka_destroy();
    return SUCCESS;
}

/** {{{ proto void DOMDocument::__construct( string $brokers );
    Constructor, expects a comma-separated list of brokers to connect to
*/
PHP_METHOD(Kafka, __construct)
{
    char *brokers = "localhost:9092";
    int brokers_len;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|s",
            &brokers, &brokers_len) == FAILURE) {
        return;
    }

    kafka_connect(brokers);
}
/* }}} end Kafka::__construct */

/* {{{ proto bool Kafka::isConnected( void )
    returns true if kafka connection is active, fals if not
*/
PHP_METHOD(Kafka, isConnected)
{
    if (kafka_is_connected()) {
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
    kafka_destroy();
}
/* }}} end Kafka::__destruct */

/* {{{ proto Kafka Kafka::set_partition( int $partition );
    Set partition (used by consume method)
    This method is deprecated, in favour of the more PSR-compliant
    Kafka::setPartition
*/
PHP_METHOD(Kafka, set_partition)
{
    zval *partition;

    if (
            zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &partition) == FAILURE
        ||
            Z_TYPE_P(partition) != IS_LONG
    ) {
        zend_throw_exception(BASE_EXCEPTION, "Partition is expected to be an int", 0 TSRMLS_CC);
        return;
    }
    kafka_set_partition(Z_LVAL_P(partition));
    //update partition property, so we can check to see if it's set when consuming
    zend_update_property(kafka_ce, getThis(), "partition", sizeof("partition") -1, partition TSRMLS_CC);
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
        zend_throw_exception(BASE_EXCEPTION, "Kafka::setLogLevel expects argument to be an int", 0 TSRMLS_CC);
        return;
    }
    if (
        Z_LVAL_P(log_level) != PHP_KAFKA_LOGLEVEL_ON
        &&
        Z_LVAL_P(log_level) != PHP_KAFKA_LOGLEVEL_OFF
    ) {
        zend_throw_exception(BASE_EXCEPTION, "Invalid argument, use Kafka::LOG_* constants", 0 TSRMLS_CC);
        return;
    }
    kafka_set_log_level(Z_LVAL_P(log_level));
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::setLogLevel */

/* {{{ proto Kafka Kafka::setPartition( int $partition );
    Set partition to use for Kafka::consume calls
*/
PHP_METHOD(Kafka, setPartition)
{
    zval *partition;

    if (
            zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &partition) == FAILURE
        ||
            Z_TYPE_P(partition) != IS_LONG
    ) {
        zend_throw_exception(BASE_EXCEPTION, "Partition is expected to be an int", 0 TSRMLS_CC);
        return;
    }
    kafka_set_partition(Z_LVAL_P(partition));
    zend_update_property(kafka_ce, getThis(), "partition", sizeof("partition") -1, partition TSRMLS_CC);
    //return $this
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::setPartition */

/* {{{ proto array Kafka::getTopics( void )
    Get all existing topics
*/
PHP_METHOD(Kafka, getTopics)
{
    array_init(return_value);
    kafka_get_topics(return_value);
}
/* }}} end Kafka::getTopics */

/* {{{ proto Kafka Kafka::setBrokers ( string $brokers)
    Set brokers on-the-fly
*/
PHP_METHOD(Kafka, setBrokers)
{
    zval *brokers;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z",
            &brokers) == FAILURE) {
        return;
    }
    if (Z_TYPE_P(brokers) != IS_STRING) {
        zend_throw_exception(BASE_EXCEPTION, "Kafka::setBrokers expects argument to be a string", 0 TSRMLS_CC);
        return;
    }
    kafka_destroy();
    kafka_connect(Z_STRVAL_P(brokers));
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} end Kafka::setBrokers */

/* {{{ proto array Kafka::getPartitionsForTopic( string $topic )
    Get an array of available partitions for a given topic
*/
PHP_METHOD(Kafka, getPartitionsForTopic)
{
    char *topic = NULL;
    int topic_len = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &topic, &topic_len) == FAILURE) {
        return;
    }
    array_init(return_value);
    kafka_get_partitions(return_value, topic);
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
    int *offsets = NULL,
        i;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &topic, &topic_len) == FAILURE) {
        return;
    }
    kafka_r = kafka_partition_offsets(&offsets, topic);
    if (kafka_r < 1) {
        const char *msg = kafka_r == 1 ? "Failed to get metadata" : "unknown partition count (or mem-error)";
        zend_throw_exception(
            BASE_EXCEPTION,
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

/* {{{ proto bool Kafka::disconnect( void );
    Disconnects kafka, returns false if disconnect failed
    Warning: producing a new message will reconnect to the initial brokers
*/
PHP_METHOD(Kafka, disconnect)
{
    kafka_destroy();
    if (kafka_is_connected()) {
        RETURN_FALSE;
    }
    RETURN_TRUE;
}
/* }}} end Kafka::disconnect */

/* {{{ proto Kafka Kafka::produce( string $topic, string $message);
    Produce a message, returns int (partition used to produce)
    or false if something went wrong
*/
PHP_METHOD(Kafka, produce)
{
    zval *object = getThis(),
        *partition;
    char *topic;
    char *msg;
    int topic_len;
    int msg_len;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss",
            &topic, &topic_len,
            &msg, &msg_len) == FAILURE) {
        return;
    }

    kafka_produce(topic, msg, msg_len);
    RETURN_ZVAL(object, 1, 0);
}
/* }}} end Kafka::produce */

/* {{{ proto array Kafka::consume( string $topic, [ mixed $offset = 0 [, int $length = 1] ] );
    Consumes 1 or more ($length) messages from the $offset (default 0)
*/
PHP_METHOD(Kafka, consume)
{
    zval *object = getThis(),
        *partition;
    char *topic;
    int topic_len;
    char *offset;
    int offset_len;
    long count = 0;
    zval *item_count;

    partition = zend_read_property(kafka_ce, object, "partition", sizeof("partition") -1, 0 TSRMLS_CC);
    if (Z_TYPE_P(partition) == IS_NULL) {
        //TODO: throw exception, trigger error, fallback to default (0) partition...
        //for now, default to 0
        kafka_set_partition(0);
        ZVAL_LONG(partition, 0);
        //update property value ->
        zend_update_property(kafka_ce, object, "partition", sizeof("partition") -1, partition TSRMLS_CC);
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|sz",
            &topic, &topic_len,
            &offset, &offset_len,
            &item_count) == FAILURE) {
        return;
    }
    if (Z_TYPE_P(item_count) == IS_STRING && strcmp(Z_STRVAL_P(item_count), PHP_KAFKA_OFFSET_END) == 0) {
        count = -1;
    } else if (Z_TYPE_P(item_count) == IS_LONG) {
        count = Z_LVAL_P(item_count);
    } else {}//todo throw exception?

    array_init(return_value);
    kafka_consume(return_value, topic, offset, count);
}
/* }}} end Kafka::consume */
