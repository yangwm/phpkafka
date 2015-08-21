#include "php_kafka.h"
#include "topic.h"
#include "broker.h"
#include "queue.h"
#include "kafka_exception.h"

static zend_object_handlers topic_handlers;

zend_class_entry *topic_ce;

/* {{{ external, then static binding functions, the actual rdkafka stuff here */

int kafka_open_topic(kafka_topic *topic)
{
    if (topic->conn == NULL)
    {//this shouldn't happen... ever!
        zend_throw_exception(kafka_exception_ce, "No connection available", 0 TSRMLS_CC);
        return -1;
    }
    rd_kafka_topic_t *rkt = NULL;
    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    char errstr[512];

    if (topic->rk_type == RD_KAFKA_CONSUMER)
    {
        if (rd_kafka_topic_conf_set(topic_conf, "auto.commit.enable", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        {
            zend_throw_exception(kafka_exception_ce, errstr, 0 TSRMLS_CC);
            return -1;
        }
    }
    /* Create topic */
    rkt = rd_kafka_topic_new(topic->conn, topic->topic_name, topic_conf);
    if (!rkt)
    {
        rd_kafka_topic_conf_destroy(topic_conf);
        zend_throw_exception(kafka_exception_ce, "Failed to open topic", 0 TSRMLS_CC);
        return -1;
    }
    topic->topic = rkt;
    topic->config = topic_conf;
    return 0;
}
/* }}} end static bind functions */

ZEND_BEGIN_ARG_INFO_EX(arginf_kafkatopic__construct, 0, 0, 2)
    ZEND_ARG_OBJ_INFO(0, connection, Kafka, 0)
    ZEND_ARG_INFO(0, topicName)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

/* {{{ proto KafkaTopic KafkaTopic::__construct( Kafka $connection, string $topicName[, int $mode = Kafka::MODE_CONSUMER] )
    Open connection to specific kafka topic
*/
PHP_METHOD(KafkaTopic, __construct)
{
    zval *obj = getThis(),
        *kafka;
    char *topic_name;
    int topic_name_len;
    long mode = 0;
    kafka_connection *connection = NULL;
    kafka_topic *topic = zend_object_store_get_object(obj TSRMLS_CC);
    rd_kafka_t *tmp = NULL;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "Os|l", &kafka, broker_ce, &topic_name, &topic_name_len, &mode) != SUCCESS)
        return;
    if (!topic_name_len)
    {
        zend_throw_exception(kafka_exception_ce, "No topic name passed", 0 TSRMLS_CC);
        return;
    }
    //validate mode && topic passed...
    if (mode != PHP_KAFKA_MODE_CONSUMER && mode != PHP_KAFKA_MODE_PRODUCER)
    {
        zend_throw_exception(kafka_exception_ce, "Invalid mode argument passed, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    connection = zend_object_store_get_object(kafka TSRMLS_CC);
    if (mode == PHP_KAFKA_MODE_CONSUMER)
    {
        //we need to work on this, perhaps link a connect method as "extern" (cf kafka_get_connection in broker.c)
        if (connection->consumer == NULL)
        {
            zend_throw_exception(kafka_exception_ce, "Passed Kafka instance does not have a consumer connection ready, call connect manually", 0 TSRMLS_CC);
            return;
        }
        topic->conn = connection->consumer;
        connection->consumer = NULL;//remove connection, used for this topic!
        topic->rk_type = RD_KAFKA_CONSUMER;
    }
    else
    {
        if (connection->producer == NULL)
        {
            zend_throw_exception(kafka_exception_ce, "Passed Kafka instance does not have a producer connection ready, call connect manually", 0 TSRMLS_CC);
            return;
        }
        topic->conn = connection->producer;
        connection->producer = NULL;
        topic->rk_type = RD_KAFKA_PRODUCER;
    }
    topic->topic_name = estrdup(topic_name);
    //if init failed, see if we can't restore the connection to the Kafka instance?
    if (kafka_open_topic(topic))
    {
        if (topic->rk_type == RD_KAFKA_PRODUCER)
            connection->producer = topic->conn;
        else
            connection->consumer = topic->conn;
        topic->conn = NULL;
        efree(topic->topic_name);
        return;
    }
}
/* }}} end KafkaTopic::__construct */

ZEND_BEGIN_ARG_INFO(arginf_kafkatopic_get_name, 0)
ZEND_END_ARG_INFO()

/* {{{ proto string KafkaTopic::getName( void )
    returns name of the current topic
*/
PHP_METHOD(KafkaTopic, getName)
{
    zval *this = getThis();
    kafka_topic *topic = zend_object_store_get_object(this TSRMLS_CC);
    RETURN_STRING(topic->topic_name, 1);
}
/* }}} end proto KafkaTopic::getName */


//methods
static
zend_function_entry topic_methods[] = {
    PHP_ME(KafkaTopic, __construct, arginf_kafkatopic__construct, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(KafkaTopic, getName, arginf_kafkatopic_get_name, ZEND_ACC_PUBLIC)
    {NULL,NULL,NULL}
};

static
void topic_free_topic(void *obj TSRMLS_DC)
{
    kafka_topic *topic = obj;
    if (topic->topic_name)
        efree(topic->topic_name);
    if (topic->conn)
    {
        if (topic->meta)
            rd_kafka_metadata_destroy(topic->meta);
        if (topic->topic)
            rd_kafka_config_destroy(topic->topic);
        rd_kafka_destroy(topic->conn);
        rd_kafka_wait_destroyed(5);//not sure how long we need to wait here
    }
    zend_object_std_dtor(&topic->std TSRMLS_CC);
    efree(topic);
}

static
zend_object_value topic_create_handler(zend_class_entry *type TSRMLS_DC)
{
    zend_object_value retval;

    kafka_topic *topic = emalloc(sizeof *topic);
    memset(topic, 0, sizeof *topic);
    zend_obj_std_init(&topic->std, type TSRMLS_CC);
    //class is final, so extending needn't apply here
#if PHP_VERSION_ID >= 50400
    object_properties_init(&topic->std, type);
#else
    zend_hash_copy(
        topic->ce.properties,
        &type->default_properties,
        (copy_ctor_func_t) zval_add_ref,
        NULL, sizeof(zval *)
    );
#endif
    retval.handle = zend_objects_store_put(
        topic,
        (zend_objects_store_dtor_t) zend_objects_destroy_object,
        (zend_objects_free_object_storage_t)topic_free_topic,
        NULL TSRMLS_CC
    );
    retval.handlers = &topic_handlers;
    return retval;
}

void kafka_init_topic(INIT_FUNC_ARGS)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "KafkaTopic", topic_methods);
    ce.create_object = topic_create_handler;
    topic_ce = zend_register_internal_class(&ce TSRMLS_CC);
    topic_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
    topic_ce->create_object = topic_create_handler;
    //default handlers
    memcpy(&topic_handlers, zend_get_std_object_handlers(), sizeof topic_handlers);
    topic_handlers.clone_obj = NULL;
}
