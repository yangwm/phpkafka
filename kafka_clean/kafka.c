/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2015 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author:                                                              |
  +----------------------------------------------------------------------+
*/

/* $Id$ */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_kafka.h"
#include "librdkafka/rdkafka.h"
#include "broker.h"
#include "topic.h"
#include "queue.h"
#include "kafka_exception.h"


/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(kafka)
{
    kafka_init_broker(INIT_FUNC_ARGS_PASSTHRU);
    kafka_init_topic(INIT_FUNC_ARGS_PASSTHRU);
    kafka_init_queue(INIT_FUNC_ARGS_PASSTHRU);
    kafka_init_exception(INIT_FUNC_ARGS_PASSTHRU);
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(kafka)
{
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
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
/* }}} */

/* {{{ kafka_module_entry
 */
zend_module_entry kafka_module_entry = {
	STANDARD_MODULE_HEADER,
	"kafka",
	NULL,
	PHP_MINIT(kafka),
	PHP_MSHUTDOWN(kafka),
	NULL,		/* Replace with NULL if there's nothing to do at request start */
	NULL,	/* Replace with NULL if there's nothing to do at request end */
	PHP_MINFO(kafka),
	PHP_KAFKA_VERSION,
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
