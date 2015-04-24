#include "topic.h"

//topic specific method list
zend_function_entry topic_methods[] = {
    PHP_ME(KafkaTopic, __construct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(KafkaTopic, getMode, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, consume, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, produce, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, consumeBatch, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, produceBatch, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(KafkaTopic, __destruct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_DTOR)
};

void kafka_init_topic(INIT_FUNC_ARGS)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "KafkaTopic", topic_methods);
}
