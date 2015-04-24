#include "topic.h"

PHP_METHOD(KafkaTopic, __construct)
{
    //get attached struct -> use passed connection to obtain
    //topic, opened in the corresponding mode
    zval *kafka,
         *mode;
    char *topic = NULL;
    int topic_len = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "zzs", &kafka, &mode, &topic, &topic_len) == FAILURE)
    {
        return;
    }
    //connect...
}

PHP_METHOD(KafkaTopic, getMode)
{
    //get struct
    RETURN_LONG(1);
}

PHP_METHOD(KafkaTopic, consume)
{
    //get struct + check mode
    //throw exception or return return_value
}

PHP_METHOD(KafkaTopic, consumeBatch)
{
    //get struct, check mode, throw exception if required
    //if not, return_value will be set accordingly
}

PHP_METHOD(KafkaTopic, produce)
{
    //same as consume methods
    RETURN_ZVAL(getThis(), 1, 0);
}

PHP_METHOD(KafkaTopic, produceBatch)
{
    //same-old
    RETURN_ZVAL(getThis(), 1, 0);
}

PHP_METHOD(KafkaTopic, __destruct)
{
    //chec struct + get handle on topics for
    //connection we used, cleanup if required...
}

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
