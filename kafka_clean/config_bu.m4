dnl $Id$
dnl config.m4 for extension kafka

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

dnl PHP_ARG_WITH(kafka, for kafka support,
dnl Make sure that the comment is aligned:
dnl [  --with-kafka             Include kafka support])

dnl Otherwise use enable:

dnl PHP_ARG_ENABLE(kafka, whether to enable kafka support,
dnl Make sure that the comment is aligned:
dnl [  --enable-kafka           Enable kafka support])

if test "$PHP_KAFKA" != "no"; then
  dnl Write more examples of tests here...

  dnl # --with-kafka -> check with-path
  dnl SEARCH_PATH="/usr/local /usr"     # you might want to change this
  dnl SEARCH_FOR="/include/kafka.h"  # you most likely want to change this
  dnl if test -r $PHP_KAFKA/$SEARCH_FOR; then # path given as parameter
  dnl   KAFKA_DIR=$PHP_KAFKA
  dnl else # search default path list
  dnl   AC_MSG_CHECKING([for kafka files in default path])
  dnl   for i in $SEARCH_PATH ; do
  dnl     if test -r $i/$SEARCH_FOR; then
  dnl       KAFKA_DIR=$i
  dnl       AC_MSG_RESULT(found in $i)
  dnl     fi
  dnl   done
  dnl fi
  dnl
  dnl if test -z "$KAFKA_DIR"; then
  dnl   AC_MSG_RESULT([not found])
  dnl   AC_MSG_ERROR([Please reinstall the kafka distribution])
  dnl fi

  dnl # --with-kafka -> add include path
  dnl PHP_ADD_INCLUDE($KAFKA_DIR/include)

  dnl # --with-kafka -> check for lib and symbol presence
  dnl LIBNAME=kafka # you may want to change this
  dnl LIBSYMBOL=kafka # you most likely want to change this 

  dnl PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  dnl [
  dnl   PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $KAFKA_DIR/$PHP_LIBDIR, KAFKA_SHARED_LIBADD)
  dnl   AC_DEFINE(HAVE_KAFKALIB,1,[ ])
  dnl ],[
  dnl   AC_MSG_ERROR([wrong kafka lib version or lib not found])
  dnl ],[
  dnl   -L$KAFKA_DIR/$PHP_LIBDIR -lm
  dnl ])
  dnl
  dnl PHP_SUBST(KAFKA_SHARED_LIBADD)

  PHP_NEW_EXTENSION(kafka, kafka.c, $ext_shared)
fi
