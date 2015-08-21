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

#ifndef PHP_KAFKA_H
#define PHP_KAFKA_H

#include <php.h>
#include <zend_exceptions.h>
extern zend_module_entry kafka_module_entry;
#define phpext_kafka_ptr &kafka_module_entry

#define PHP_KAFKA_VERSION "0.1.0" /* Replace with version number for your extension */

#ifdef PHP_WIN32
#	define PHP_KAFKA_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#	define PHP_KAFKA_API __attribute__ ((visibility("default")))
#else
#	define PHP_KAFKA_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

#ifdef ZTS
#define KAFKA_G(v) TSRMG(kafka_globals_id, zend_kafka_globals *, v)
#else
#define KAFKA_G(v) (kafka_globals.v)
#endif

//get base exception
#ifndef BASE_EXCEPTION
#if (PHP_MAJOR_VERSION < 5) || ( ( PHP_MAJOR_VERSION == 5 ) && (PHP_MINOR_VERSION < 2) )
#define BASE_EXCEPTION zend_exception_get_default()
#else
#define BASE_EXCEPTION zend_exception_get_default(TSRMLS_C)
#endif
#endif

#define REGISTER_KAFKA_CLASS_CONST_STRING(ce, name, value) \
    zend_declare_class_constant_stringl(ce, name, sizeof(name)-1, value, sizeof(value)-1)
#define REGISTER_KAFKA_CLASS_CONST_LONG(ce, name, value) \
    zend_declare_class_constant_long(ce, name, sizeof(name)-1, value)
#define REGISTER_KAFKA_CLASS_CONST(ce, c_name, type) \
    REGISTER_KAFKA_CLASS_CONST_ ## type(ce, #c_name, PHP_KAFKA_ ## c_name)



#endif	/* PHP_KAFKA_H */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
