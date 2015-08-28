#include "php_kafka.h"
#include "broker.h"
#include "topic.h"
#include "kafka_exception.h"

static zend_object_handlers broker_handlers;
zend_class_entry *broker_ce;

//callback structs, opaque pointers mainly:
struct produce_cb_params {
    int msg_count;
    int err_count;
    int offset;
    int partition;
    int errmsg_len;
    char *err_msg;
};

/* {{{ binding functions (get connection, get topics etc...)
    These are static, so let's ensure we've declared them before calling
 */
static
void kafka_produce_cb_simple(rd_kafka_t *rk, void *payload, size_t len, int err_code, void *opaque, void *msg_opaque)
{
    if (msg_opaque)
    {
        struct produce_cb_params *params = msg_opaque;
        params->msg_count -=1;
    }
}

static
void kafka_produce_detailed_cb(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque)
{
    struct produce_cb_params *params = opaque;
    if (params)
    {
        params->msg_count -= 1;
    }
    if (msg->err)
    {
        int offset = params->errmsg_len,
            err_len = 0;
        const char *errstr = rd_kafka_message_errstr(msg);
        err_len = strlen(errstr);
        if (params)
        {
            params->err_count += 1;
            params->err_msg = realloc(
                params->err_msg,
                (offset + err_len + 2) * sizeof params->err_msg
            );
            if (params->err_msg == NULL)
            {
                params->errmsg_len = 0;
            }
            else
            {
                strcpy(
                    params->err_msg + offset,
                    errstr
                );
                offset += err_len;//get new strlen
                params->err_msg[offset] = '\n';//add new line
                ++offset;
                params->err_msg[offset] = '\0';//ensure zero terminated string
            }
        }
        return;
    }
    if (params)
    {
        params->offset = msg->offset;
        params->partition = msg->partition;
    }
}

static
void connection_error_cb(rd_kafka_t *conn, int err, const char *reason, void *opaque)
{
    kafka_connection *internal = opaque;
    int errcode = E_WARNING;
    if (conn)
    {
        if (internal->consumer == conn)
            internal->consumer = NULL;
        else if (internal->producer == conn)
            internal->producer = NULL;
        rd_kafka_destroy(conn);
        rd_kafka_wait_destroyed(0);
    }
    if (internal->err_mode)
    {
        errcode = E_ERROR;
    }
    zend_error(errcode, "connection failed: %s", reason);
} 

static
int kafka_get_connection(kafka_connection *connection, rd_kafka_type_t type)
{
    rd_kafka_t *r = NULL;
    int retval = 0;
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    //set error callback
    rd_kafka_conf_set_error_cb(conf, connection_error_cb);
    //pass internal pointer to error callback
    rd_kafka_conf_set_opaque(conf, connection);
    //PHP is stateless, broker should store offsets
    rd_kafka_conf_set(
        conf, "offset.store.method","broker", errstr, sizeof errstr
    );

    if (type == RD_KAFKA_CONSUMER)
    {
        if (connection->queue_buffer)
            rd_kafka_conf_set(conf, "queued.min.messages", connection->queue_buffer, NULL, 0);
        r = rd_kafka_new(type, conf, errstr, sizeof errstr);
        if (!r)
        {
            if (connection->err_mode)
            {
                zend_throw_exception(kafka_exception_ce, errstr, 0 TSRMLS_CC);
                retval = -1;//negative signals exception has been thrown
            }
            else
            {
                zend_error(E_WARNING, "Connection failed: %s", errstr);
                retval = 1;
            }
            //destroy config, no connection to use it...
            rd_kafka_conf_destroy(conf);
            return retval;
        }
        if (!rd_kafka_brokers_add(r, connection->brokers))
        {
            if (connection->err_mode)
            {
                zend_throw_exception(kafka_exception_ce, "Failed to connect to brokers", 0 TSRMLS_CC);
                retval = -1;
            }
            else
            {
                zend_error(E_WARNING, "Failed to connect to brokers %s", connection->brokers);
                retval = 1;
            }
            rd_kafka_destroy(r);
            r = NULL;
        }
        connection->consumer = r;
        return retval;
    }
    if (connection->compression || strcmp(PHP_KAFKA_COMPRESSION_NONE, connection->compression))
    {
        rd_kafka_conf_res_t result = rd_kafka_conf_set(
            conf, "compression.codec",connection->compression, errstr, sizeof errstr
        );
        if (result != RD_KAFKA_CONF_OK)
        {
            if (connection->err_mode)
            {
                zend_throw_exception(kafka_exception_ce, errstr, 0 TSRMLS_CC);
                retval = -1;
            }
            else
            {
                zend_error(E_WARNING, "Connection failed: %s", errstr);
                retval = 1;
            }
            rd_kafka_conf_destroy(conf);
            return retval;
        }
    }
    if (connection->retry_count)
    {
        rd_kafka_conf_res_t result = rd_kafka_conf_set(
            conf, "message.send.max.retries",connection->retry_count, errstr, sizeof errstr
        );
        if (result != RD_KAFKA_CONF_OK)
        {
            if (connection->err_mode)
            {
                zend_throw_exception(kafka_exception_ce, errstr, 0 TSRMLS_CC);
                retval = -1;
            }
            else
            {
                zend_error(E_WARNING, "Connection failed: %s", errstr);
                retval = 1;
            }
            rd_kafka_conf_destroy(conf);
            return retval;
        }
    }
    if (connection->retry_interval)
    {
        rd_kafka_conf_res_t result = rd_kafka_conf_set(
            conf, "retry.backoff.ms",connection->retry_interval, errstr, sizeof errstr
        );
        if (result != RD_KAFKA_CONF_OK)
        {
            if (connection->err_mode)
            {
                zend_throw_exception(kafka_exception_ce, errstr, 0 TSRMLS_CC);
                retval = -1;
            }
            else
            {
                zend_error(E_WARNING, "Connection failed: %s", errstr);
                retval = 1;
            }
            rd_kafka_conf_destroy(conf);
            return retval;
        }
    }
    if (connection->delivery_confirm_mode == 1)
        rd_kafka_conf_set_dr_cb(conf, kafka_produce_cb_simple);
    else if (connection->delivery_confirm_mode == 2)
        rd_kafka_conf_set_dr_msg_cb(conf, kafka_produce_detailed_cb);
    r = rd_kafka_new(type, conf, errstr, sizeof errstr);
    if (!r)
    {
        if (connection->err_mode)
        {
            zend_throw_exception(kafka_exception_ce, errstr, 0 TSRMLS_CC);
            retval = -1;
        }
        zend_error(E_WARNING, "Connection failed: %s", errstr);
        retval = 1;
        //destroy config, no connection to use it...
        rd_kafka_conf_destroy(conf);
        return retval;
    }
    if (!rd_kafka_brokers_add(r, connection->brokers))
    {
        if (connection->err_mode)
        {
            zend_throw_exception(kafka_exception_ce, "Failed to connect to brokers", 0 TSRMLS_CC);
            retval = -1;
        }
        else
        {
            zend_error(E_WARNING, "Failed to connect to brokers %s", connection->brokers);
            retval = 1;
        }
        rd_kafka_destroy(r);
        return retval;
    }
    connection->producer = r;
}


//get topics + partition count
static
void kafka_get_topics(kafka_connection *connection, zval *return_value)
{
    int i;
    const struct rd_kafka_metadata *meta = NULL;
    if (connection->consumer == NULL)
    {
        i = kafka_get_connection(connection, RD_KAFKA_CONSUMER);
        if (i)
            return;
    }
    if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_metadata(connection->consumer, 1, NULL, &meta, 200)) {
        for (i=0;i<meta->topic_cnt;++i) {
            add_assoc_long(
               return_value,
               meta->topics[i].topic,
               (long) meta->topics[i].partition_cnt
            );
        }
    }
    else
    {
        if (connection->err_mode)
        {
            zend_throw_exception(kafka_exception_ce, "Failed to get metadata", 0 TSRMLS_CC);
            return;
        }
        zend_error( E_WARNING, "Failed to get metadata");
    }
    if (meta) {
        rd_kafka_metadata_destroy(meta);
    }
}
/* }}} end binding functions */

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
            zend_throw_exception(kafka_exception_ce, "Invalid option key, use class constants", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid argument for Kafka::PRODUCE_BATCH_SIZE, expected numeric value", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid argument for Kafka::CONSUME_BATCH_SIZE, expected numeric value", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid value for Kafka::RETRY_COUNT option, expected numeric value", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid value for Kafka::RETRY_INTERVAL option, expected numeric value", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid value for Kafka::CONFIRM_DELIVERY, use Kafka::CONFIRM_* constants", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid value for Kafka::QUEUE_BUFFER_SIZE, expected numeric value", 0 TSRMLS_CC);
                        return -1;
                    }
                    break;
                case PHP_KAFKA_COMPRESSION_MODE:
                    if (Z_TYPE_PP(entry) != IS_STRING)
                    {
                        zend_throw_exception(kafka_exception_ce, "Invalid type for Kafka::COMPRESSION_MODE option, use Kafka::COMPRESSION_* constants", 0 TSRMLS_CC);
                        return -1;
                    }
                    if (
                        !strcmp(Z_STRVAL_PP(entry), PHP_KAFKA_COMPRESSION_GZIP)
                        &&
                        !strcmp(Z_STRVAL_PP(entry), PHP_KAFKA_COMPRESSION_NONE)
                        &&
                        !strcmp(Z_STRVAL_PP(entry), PHP_KAFKA_COMPRESSION_SNAPPY)
                    ) {
                        zend_throw_exception(kafka_exception_ce, "Invalid value for Kafka::COMPRESSION_MODE, use Kafka::COMPRESSION_* constants", 0 TSRMLS_CC);
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
                        zend_throw_exception(kafka_exception_ce, "Invalid value for Kafka::LOGLEVEL option, use Kafka::LOG_* constants", 0 TSRMLS_CC);
                        return -1;
                    }
                    connection->err_mode = Z_LVAL_PP(entry);
                    break;
            }
        }
        zend_hash_move_forward_ex(Z_ARRVAL_P(arr), &pos);
    }
    return 0;
}

//general arginf => void
ZEND_BEGIN_ARG_INFO(arginf_kafka_void, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka__constr, 0, 0, 1)
    ZEND_ARG_INFO(0, brokers)
    ZEND_ARG_ARRAY_INFO(0, options, 1)
ZEND_END_ARG_INFO()

/* {{{ proto Kafka Kafka::__construct( string $brokers[, array $options  = null ])
        Kafka constructor
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
    zend_replace_error_handling(EH_THROW, kafka_exception_ce, NULL TSRMLS_CC);
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
    }
    zend_replace_error_handling(EH_NORMAL, NULL, NULL TSRMLS_CC);
}
/* }}} end proto Kafka::__construct */

/* {{{ proto string Kafka::getCompression( void )
 * Get current compression mode
 */
PHP_METHOD(Kafka, getCompression)
{
    zval *obj = getThis();
    kafka_connection *connection = (kafka_connection *) zend_object_store_get_object(obj TSRMLS_CC);
    if (!connection->compression)
        RETURN_STRING(PHP_KAFKA_COMPRESSION_NONE, 1);
    RETURN_STRING(connection->compression, 1);
}
/* }}} end proto Kafka::getCompression */

ZEND_BEGIN_ARG_INFO(arginf_kafka_set_brokers, 0)
    ZEND_ARG_INFO(0, brokers)
ZEND_END_ARG_INFO()

/* {{{ proto Kafka Kafka::setBrokers( string $brokers )
 * Set (new) brokers to connect to, resets opened connections
 */
PHP_METHOD(Kafka, setBrokers)
{
    zval *obj = getThis();
    kafka_connection *conn = (kafka_connection *) zend_object_store_get_object(obj TSRMLS_CC);
    char *brokers;
    int broker_len;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &brokers, &broker_len) != SUCCESS)
        return;
    if (broker_len < 1)
    {
        zend_throw_exception(kafka_exception_ce, "Empty string not allowed", 0 TSRMLS_CC);
        return;
    }
    if (conn->brokers)
    {//brokers were set, connections might be open...
        if (conn->consumer)
        {
            rd_kafka_destroy(conn->consumer);
            rd_kafka_wait_destroyed(10);//shouldn't block too long
        }
        if (conn->producer)
        {
            rd_kafka_destroy(conn->producer);
            rd_kafka_wait_destroyed(50);//wait a bit longer for produce calls
        }
        efree(conn->brokers);
    }
    conn->brokers = estrdup(brokers);
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto Kafka::setBrokers */

ZEND_BEGIN_ARG_INFO_EX(arginf_kafka_connect, 0, 0, 0)
    ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

/* {{{ proto mixed Kafka::isConnected( [ int $mode = -1 ] )
 * Returns bool if mode is given (KAFKA::MODE_*)
 * array with Kafka::MODE_* as keys, each with bool values, true for connected
 */
PHP_METHOD(Kafka, isConnected)
{
    zval *obj = getThis();
    kafka_connection *conn = (kafka_connection *) zend_object_store_get_object(obj TSRMLS_CC);
    long mode = -1;
    int connected = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|l", &mode) != SUCCESS)
        return;
    if (mode != -1)
    {
        if (mode == PHP_KAFKA_MODE_CONSUMER)
        {
            if (conn->consumer)
            {
                RETURN_TRUE;
            }
            RETURN_FALSE;
        }
        if (mode == PHP_KAFKA_MODE_PRODUCER)
        {
            if (conn->producer)
            {
                RETURN_TRUE;
            }
            RETURN_FALSE;
        }
        zend_throw_exception(kafka_excpetion_ce, "Invalid mode, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    array_init(return_value);
    //add mode keys + bool values:
    if (conn->consumer)
    {
        connected = 1;
    }
    add_index_bool(return_value, PHP_KAFKA_MODE_CONSUMER, connected);
    if (conn->producer)
    {
        connected = 1;
    }
    else
    {
        connected = 0;
    }
    add_index_bool(return_value, PHP_KAFKA_MODE_PRODUCER, connected);
}
/* }}} end proto Kafka::isConnected */

/* {{{ proto Kafka Kafka::connect( [ int $mode = -1 ] )
 * preload specific connection, or both if no mode argument is passed
 */
PHP_METHOD(Kafka, connect)
{
    zval *obj = getThis();
    kafka_connection *conn = zend_object_store_get_object(obj TSRMLS_CC);
    long mode = -1;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|l", &mode) != SUCCESS)
        return;
    if (mode == -1 || mode == PHP_KAFKA_MODE_CONSUMER)
    {
        if (conn->consumer == NULL)
        {//do not connect twice...
            if (kafka_get_connection(conn, RD_KAFKA_CONSUMER))
            {//an exception was thrown, stop here
                return;
            }
        }
        if (mode == -1)
        {
            mode = PHP_KAFKA_MODE_PRODUCER;
        }
    }
    if (mode == PHP_KAFKA_MODE_PRODUCER)
    {
        if (conn->producer == NULL)
        {//again: don't connect twice
            if (kafka_get_connection(connection, RD_KAFKA_CONSUMER))
            {//an exception was thrown, stop here
                return;
            }
        }
    }
    else
    {//mode was invalid, stop here...
        zend_throw_exception(kafka_exception_ce, "Invalid mode argument passed, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto Kafka::connect */

/* {{{ proto Kafka Kafka::disconnect( [ int $mode = -1 ] )
 * close specific connection, or both if no mode argument is passed
 */
PHP_METHOD(Kafka, disconnect)
{
    zval *obj = getThis();
    kafka_connection *conn = (kafka_connection *) zend_object_store_get_object(obj TSRMLS_CC);
    long mode = -1;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|l", &mode) != SUCCESS)
        return;
    if (mode == -1 || mode == PHP_KAFKA_MODE_CONSUMER)
    {
        if (conn->consumer)
        {//do not disconnect NULL connection
            rd_kafka_destroy(conn->consumer);
            rd_kafka_wait_destroyed(10);//shouldn't block too long
            conn->consumer = NULL;
        }
        if (mode == -1)
        {
            mode = PHP_KAFKA_MODE_PRODUCER;
        }
    }
    if (mode == PHP_KAFKA_MODE_PRODUCER)
    {
        if (conn->producer)
        {
            rd_kafka_destroy(conn->producer);
            rd_kafka_wait_destroyed(10);//shouldn't block too long
            conn->producer = NULL;
        }
    }
    else
    {
        zend_throw_exception(kafka_exception_ce, "Invalid mode argument passed, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    RETURN_ZVAL(obj, 1, 0);
}
/* }}} end proto Kafka::disconnect */

/* {{{ proto array Kafka::getTopics( void )
    Get all existing topics
*/
PHP_METHOD(Kafka, getTopics)
{
    zval *obj = getThis();
    kafka_connection *connection = zend_object_store_get_object(obj TSRMLS_CC);
    if (connection->brokers == NULL && connection->consumer == NULL)
    {
        zend_throw_exception(kafka_exception_ce, "No brokers to get topics from", 0 TSRMLS_CC);
        return;
    }
    array_init(return_value);
    kafka_get_topics(connection, return_value);
}
/* }}} end Kafka::getTopics */

/* {{{ proto KafkaTopic Kafka::getTopic( string $topicName, int $mode)
 * Return instance of KafkaTopic to use for producing or consuming
 */
PHP_METHOD(Kafka, getTopic)
{
    zval *obj = getThis();
    kafka_connection *connection = zend_object_store_get_object(obj TSRMLS_CC);
    kafka_topic *topic;
    char *topic_name;
    int topic_name_len, errcode;
    long mode = 0;//PHP_KAFKA_MODE_CONSUMER default?
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl", &topic_name, &topic_name_len, &mode) != SUCCESS)
        return;//fatal
    //topic_name argument validation is handled by KafkaTopic constructor
    if (mode != PHP_KAFKA_MODE_CONSUMER && mode != PHP_KAFKA_MODE_PRODUCER)
    {
        zend_throw_exception(kafka_exception_ce, "Invalid mode, use Kafka::MODE_* constants", 0 TSRMLS_CC);
        return;
    }
    if (mode == PHP_KAFKA_MODE_CONSUMER)
    {
        if (connection->consumer == NULL)
        {
            errcode = kafka_get_connection(connection, RD_KAFKA_CONSUMER);
            if (errcode)
            {//exception was already thrown/warning was emitted
                if (errcode == 1)
                {//1 means a warning was issued, and no exception was thrown
                    ZVAL_NULL(return_value);
                }
                return;
            }
        }
    }
    else // if (mode == PHP_KAFKA_MODE_PRODUCER)
    {//assume
        if (connection->producer == NULL)
        {
            errcode = kafka_get_connection(connection, RD_KAFKA_PRODUCER);
            if (errcode)
            {//exception was already thrown/warning was emitted
                if (errcode == 1)
                {//1 means a warning was issued, and no exception was thrown
                    ZVAL_NULL(return_value);
                }
                return;
            }
        }
    }

    //init return value to topic_ce instance
    object_init_ex(return_value, topic_ce);
    topic = zend_object_store_get_object(return_value TSRMLS_CC);
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
    //init function is currently NOT exposed, so we need a way to fix that
    //make it external? Add another translation unit with common kafka functions?
    //or call constructor as if the user wrote new KafkaTopic(Kafka, $name, $mode);?
    //using this:
    //CALL_METHOD3(KafkaTopic, __construct, return_value, return_value, obj, &z_topic_name, &z_mode);
    //for now, let's expose that init functionS
    if (kafka_open_topic(topic))
    {
        efree(topic->topic_name);
        if (mode == PHP_KAFKA_MODE_CONSUMER)
            connection->consumer = topic->conn;
        else
            connection->producer = topic->conn;
        topic->conn = NULL;
    }
}
/* }}} end Kafka::getTopic */

static zend_function_entry broker_methods[] = {
    PHP_ME(Kafka, __construct, arginf_kafka__constr, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(Kafka, getCompression, arginf_kafka_void, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, setBrokers, arginf_kafka_set_brokers, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, isConnected, arginf_kafka_connect, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, connect, arginf_kafka_connect, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, disconnect, arginf_kafka_connect, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, getTopics, arginf_kafka_void, ZEND_ACC_PUBLIC)
    {NULL,NULL,NULL}
};

//internal dtor
static
void broker_free_connection(void *object TSRMLS_DC)
{
    kafka_connection *conn = object;

    if (conn->brokers)
        efree(conn->brokers);
    if (conn->compression)
        efree(conn->compression);
    if (conn->retry_count)
        efree(conn->retry_count);
    if (conn->retry_interval)
        efree(conn->retry_interval);
    if (conn->queue_buffer);
        efree(conn->queue_buffer);
    if (conn->consumer)
    {
        rd_kafka_destroy(conn->consumer);
        rd_kafka_wait_destroyed(10);//shouldn't block too long
    }
    if (conn->producer)
    {
        rd_kafka_destroy(conn->producer);
        rd_kafka_wait_destroyed(50);//wait a bit longer for produce calls
    }
    zend_object_std_dtor(&conn->std TSRMLS_CC);
    efree(conn);
}

//pre-constructor
static
zend_object_value broker_create_handler(zend_class_entry *type TSRMLS_DC)
{
    zend_object_value retval;
    kafka_connection *connection = emalloc(sizeof *connection);
    memset(connection, 0, sizeof *connection);

    zend_obj_std_init(&connection->std, type TSRMLS_CC);
    connection->std.ce = type;
    //set defaults

#if PHP_VERSION_ID >= 50400
    object_properties_init(&connection->std, type);
#else
    zend_hash_copy(
        connection->ce.properties,
        &type->default_properties,
        (copy_ctor_func_t) zval_add_ref,
        NULL, sizeof(zval *)
    );
#endif
    retval.handle = zend_objects_store_put(
        connection,
        (zend_objects_store_dtor_t)zend_objects_destroy_object,
        (zend_objects_free_object_storage_t)broker_free_connection,
        NULL TSRMLS_CC
    );
    retval.handlers = &broker_handlers;
    return retval;
}

void kafka_init_broker(INIT_FUNC_ARGS)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "Kafka", broker_methods);

    ce.create_object = broker_create_handler;
    broker_ce = zend_register_internal_class(&ce TSRMLS_CC);

    //make final!
    broker_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
    broker_ce->create_object = broker_create_handler;//? ce.create_object?

    memcpy(&broker_handlers, zend_get_std_object_handlers(), sizeof broker_handlers);
    broker_handlers.clone_obj = NULL;
    //@todo: register constants...
    //offset constants
    REGISTER_KAFKA_CLASS_CONST(broker_ce, OFFSET_BEGIN, STRING);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, OFFSET_END, STRING);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, OFFSET_STORED, STRING);
    //compression constants
    REGISTER_KAFKA_CLASS_CONST(broker_ce, COMPRESSION_NONE, STRING);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, COMPRESSION_GZIP, STRING);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, COMPRESSION_SNAPPY, STRING);
    //mode constants
    REGISTER_KAFKA_CLASS_CONST(broker_ce, MODE_CONSUMER, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, MODE_PRODUCER, LONG);
    //partition const
    REGISTER_KAFKA_CLASS_CONST(broker_ce, PARTITION_RANDOM, LONG);
    //config constants
    REGISTER_KAFKA_CLASS_CONST(broker_ce, RETRY_COUNT, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, RETRY_INTERVAL, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, CONFIRM_DELIVERY, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, QUEUE_BUFFER_SIZE, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, COMPRESSION_MODE, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, LOGLEVEL, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, PRODUCE_BATCH_SIZE, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, CONSUME_BATCH_SIZE, LONG);
    //confirmation value constants
    REGISTER_KAFKA_CLASS_CONST(broker_ce, CONFIRM_OFF, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, CONFIRM_BASIC, LONG);
    REGISTER_KAFKA_CLASS_CONST(broker_ce, CONFIRM_EXTENDED, LONG);
}

