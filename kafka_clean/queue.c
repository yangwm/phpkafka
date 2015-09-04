#include "queue.h"
#include "kafka_exception.h"
#include "topic.h"
#include "broker.h"

zend_class_entry *queue_ce;

static zend_object_handlers queue_handlers;

/* {{{ Static functions -> actual librdkafka bindings here */
static
int kafka_consume_queue(kafka_topic *topic, kafka_queue *queue, const char *offset, int item_count)
{
    rd_kafka_t *conn = topic->conn;
    rd_kafka_topic_t *rd_topic = topic->topic;
    const struct rd_kafka_metadata *meta = topic->meta;
    if (meta == NULL)
    {
        meta = get_topic_meta(topic);
    }
}

int kafka_topic_consume_batch_queue(rd_kafka_t *conn, rd_kafka_topic_t *topic, const struct rd_kafka_metadata *meta, zval *return_value, const char *offset, int item_count, void **zval_opaque)
{//old function, new one above
    //check for NULL pointers, all arguments are required!
    if (conn == NULL || return_value == NULL || topic == NULL || offset == NULL || strlen(offset) == 0 || zval_opaque == NULL)
        return -10;
    rd_kafka_queue_t *queue = rd_kafka_queue_new(conn);
    int current, p, i = 0;
    int32_t max = 0, partition = 0;
    int64_t start;
    struct consume_cb_async_s * opaque = malloc(sizeof *opaque);
    //assign ptr to zval opaque, so we can use it to stop consuming without leaking memory
    *zval_opaque = opaque;
    if (!opaque)
        return -11;
    opaque->read_count = item_count;
    opaque->return_value = return_value;
    opaque->partition_ends = NULL;
    opaque->error_count = 0;
    p = opaque->eop = meta->topics->partition_cnt;
    opaque->conn = conn;
    opaque->queue = queue;
    opaque->meta = meta;
    struct consume_cb_params cb_params = {item_count, return_value, NULL, 0, 0, 0};

    if (!strcmp(offset, "end"))
        start = RD_KAFKA_OFFSET_END;
    else if (!strcmp(offset, "beginning"))
        start = RD_KAFKA_OFFSET_BEGINNING;
    else if (!strcmp(offset, "stored"))
        start = RD_KAFKA_OFFSET_STORED;
    else
        start = strtoll(offset, NULL, 10);

    cb_params.partition_ends = calloc(sizeof *cb_params.partition_ends, p);
    if (cb_params.partition_ends == NULL)
    {
        if (log_level)
        {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - Failed to read %s from %"PRId64" (%s)", meta->topics->topic, start, offset);
        }
        rd_kafka_queue_destroy(queue);
        return -1;
    }
    cb_params.eop = p;
    for (i=0;i<p;++i)
    {
        partition = meta->topics[0].partitions[i].id;
        if (partition > max)
        {
            max = partition;
        }
        if (rd_kafka_consume_start_queue(topic, partition, start, queue))
        {
            if (log_level)
            {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_ERR,
                    "Failed to start consuming topic %s [%"PRId32"]: %s",
                    meta->topics->topic, partition, offset
                );
            }
            opaque->eop -= 1;//one less partition to worry about
            continue;
        }
    }

    opaque->partition_ends = calloc(max, sizeof *opaque->partition_ends);
    if (!opaque->partition_ends)
    {
        for (i=0;i<p; ++i)
        {
            rd_kafka_consume_stop(topic, meta->topics->partitions[i].id);
        }
        rd_kafka_queue_destroy(queue);
        free(opaque);
        return 1;
    }

    for (i=0;i<p;++i)
    {
        partition = meta->topics->partitions[i].id;
        opaque->partition_ends[partition] = 1;
    }

    rd_kafka_consume_callback_queue(queue, 200, async_consume_queue_cb, opaque);
    return 0;
}
/* }}} end static functions */

ZEND_BEGIN_ARG_INFO_EX(arginf_kafkaqueue_constr, 0, 0, 2)
    ZEND_ARG_OBJ_INFO(0, topic, KafkaTopic, 0)
    ZEND_ARG_INFO(0, mode)
    ZEND_ARG_INFO(0, itemCount)
    ZEND_ARG_INFO(0, offset)
ZEND_END_ARG_INFO()

/* proto KafkaQueue KafkaQueue::__construct( KafkaTopic $topic[, int $itemCount = -1[, string $offset]])
 * Open new topic queue
 */
PHP_METHOD(KafkaQueue, __construct)
{
    zval *this = getThis(),
        *topic = NULL,
        *buffer;
    long item_count = -1;
    int offset_len, mode = PHP_KAFKA_MODE_CONSUMER;
    char *offset = NULL;
    kafka_queue *queue = zend_object_store_get_object(this TSRMLS_CC);
    kafka_topic *topic_internal = NULL;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "O|ls", &topic, topic_ce, &item_count, &offset, &offset_len) != SUCCESS)
        return;//fatal
    if (item_count < -1 || item_count == 0)
    {
        zend_throw_exception(
            kafka_exception_ce,
            "Invalid value for batchSize argument (-1 or positive int expected)",
            0 TSRMLS_CC
        );
        return;
    }
    //set internal array property -> this will act as the buffer for our consume calls
    if (queue->msg_arr == NULL)
    {//consume -> we need to init the target array
        MAKE_STD_ZVAL(buffer);
        array_init(buffer);
        queue->msg_arr = buffer;
    }
    else
    {//produce -> struct contains messages to produce
        buffer = queue->msg_arr;
        mode = PHP_KAFKA_MODE_PRODUCER;
    }
    zend_update_property(queue_ce, this, "queueBuffer", sizeof("queueBuffer") -1, buffer TSRMLS_CC);
    //keep a reference to the topic
    zend_update_property(queue_ce, this, "topic", sizeof("topic") -1, topic TSRMLS_CC);
    topic_internal = zend_object_store_get_object(topic TSRMLS_CC);
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
            return;
        }
    }
    if (!offset || !offset_len)
    {
        offset = PHP_KAFKA_OFFSET_STORED;
    }
    //init queue object:
    //object_init_ex(return_value, kafka_queue_ce);
    //kafka_queue *queue= (kafka_queue *) zend_object_store_get_object(return_value TSRMLS_CC);
    //consume_batch will be using the msg_array zval rather than the return_value (will be instance of KafkaQueue)
    //array_init(queue->params.msg_arr);
    if (!offset_len)
        offset = PHP_KAFKA_OFFSET_STORED;
    array_init(return_value);
    //status = kafka_topic_consume_batch(topic->conn, topic->topic, topic->meta, queue->params.msg_arr, offset, item_count, &queue->opaque);
    //@todo -> implement initial consume calls
    //status = kafka_topic_consume_batch(topic->conn, topic->topic, topic->meta, return_value, offset, item_count);
}

ZEND_BEGIN_ARG_INFO(arginf_kafkaqueue_void, 0)
ZEND_END_ARG_INFO()

/* {{{ proto bool KafkaQueue::isDone( void )
    returns true if queue is complete, false otherwise
*/
PHP_METHOD(KafkaQueue, isDone)
{
    zval *this = getThis();
    kafka_queue *queue = zend_object_store_get_object(this TSRMLS_CC);
    RETURN_BOOL(queue->is_done);
}
/* }}} end proto KafkaQueue::isDone */

/* {{{ proto KafkaTopic KafkaQueue::getTopic( void )
    get a reference to the topic instance this queue uses
*/
PHP_METHOD(KafkaQueue, getTopic)
{
    zval *topic, *obj = getThis();
    topic = zend_read_property(queue_ce, obj, "topic", sizeof("topic") - 1, 1 TSRMLS_CC);
    RETURN_ZVAL(topic, 1, 0);
}
/* }}} end KafkaQueue::getTopic */

static zend_function_entry queue_methods[] = {
    PHP_ME(KafkaQueue, __construct, arginf_kafkaqueue_constr, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(KafkaQueue, isDone, arginf_kafkaqueue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaQueue, getTopic, arginf_kafkaqueue_void, ZEND_ACC_PUBLIC)
    {NULL, NULL, NULL}
};

static
void queue_free_queue(void *obj)
{
    kafka_queue *queue = obj;
    queue->msg_arr = NULL;//references outside of this class aren't our business
    if (queue->partition_ends)
        free(queue->partition_ends);//allocated outside of PHP's memory management systems
    zend_object_std_dtor(&queue->std TSRMLS_CC);
    efree(queue);
}

static
zend_object_value queue_create_handler(zend_class_entry *type TSRMLS_DC)
{
    zend_object_value retval;

    kafka_queue *queue = emalloc(sizeof *queue);
    memset(queue, 0, sizeof *queue);
    zend_obj_std_init(&queue->std, type TSRMLS_CC);
#if PHP_VERSION_ID >= 50400
    object_properties_init(&queue->std, type);
#else
    zend_hash_copy(
        topic->ce.properties,
        &type->default_properties,
        (copy_ctor_func_t) zval_add_ref,
        NULL, sizeof(zval *)
    );
#endif
    retval.handle = zend_objects_store_put(
        queue,
        (zend_objects_store_dtor_t) zend_objects_destroy_object,
        (zend_objects_free_object_storage_t)queue_free_queue,
        NULL TSRMLS_CC
    );
    retval.handlers = &queue_handlers;
    return retval;
}

void kafka_init_queue(INIT_FUNC_ARGS)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "KafkaQueue", queue_methods);
    ce.create_object = queue_create_handler;
    queue_ce = zend_register_internal_class(&ce TSRMLS_CC);
    queue_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
    queue_ce->create_object = queue_create_handler;
    //default handlers
    memcpy(&queue_handlers, zend_get_std_object_handlers(), sizeof queue_handlers);
    queue_handlers.clone_obj = NULL;
    //add property that'll reference the topic instance we're using
    //used to keep topic in memory untill queue is done -> topic contains connection
    zend_declare_property_null(queue_ce, "topic", sizeof("topic") - 1, ZEND_ACC_PRIVATE);
    zend_declare_property_null(queue_ce, "queueBuffer", sizeof("queueBuffer") -1, ZEND_ACC_PRIVATE);
}
