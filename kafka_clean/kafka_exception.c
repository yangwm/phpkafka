#include "kafka_exception.h"
#include "php_kafka.h"

zend_class_entry *kafka_exception_ce;

void kafka_init_exception(INIT_FUNC_ARGS)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "KafkaException", NULL);
    kafka_exception_ce = zend_register_internal_class_ex(
        &ce,
        BASE_EXCEPTION,
        NULL TSRMLS_CC
    );
}
