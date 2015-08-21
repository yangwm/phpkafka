#ifndef KAFKA_EXCEPTION_H_
#define KAFKA_EXCEPTION_H_

#include <php.h>

void kafka_init_exception(INIT_FUNC_ARGS);

extern zend_class_entry *kafka_exception_ce;
#endif
