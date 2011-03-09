/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	 You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <inttypes.h>
#include <string.h>

#include "avro.h"
#include "avro_private.h"
#include "avro/allocation.h"
#include "avro/consumer.h"
#include "avro/data.h"
#include "avro/errors.h"


static void
avro_raw_resolver_free(avro_consumer_t *consumer)
{
	avro_freet(avro_consumer_t, consumer);
}


/*-----------------------------------------------------------------------
 * boolean
 */

static int
avro_raw_boolean_value(avro_consumer_t *consumer, int value,
		       void *user_data)
{
	AVRO_UNUSED(consumer);
	int  *dest = user_data;
	*dest = value;
	return 0;
}

avro_consumer_t *
avro_raw_boolean_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a boolean field if the writer is a boolean,
	 * or (TODO), a union containing a boolean.
	 */

	if (is_avro_boolean(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.boolean_value = avro_raw_boolean_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into boolean",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * bytes
 */

static int
avro_raw_bytes_value(avro_consumer_t *consumer,
		     const void *value, size_t value_len,
		     void *user_data)
{
	AVRO_UNUSED(consumer);
	avro_raw_string_t  *dest = user_data;
	avro_raw_string_set_length(dest, value, value_len);
	return 0;
}

avro_consumer_t *
avro_raw_bytes_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a bytes field if the writer is a bytes,
	 * or (TODO), a union containing a bytes.
	 */

	if (is_avro_bytes(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.bytes_value = avro_raw_bytes_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into bytes",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * double
 */

static int
avro_raw_double_value(avro_consumer_t *consumer, double value,
		      void *user_data)
{
	AVRO_UNUSED(consumer);
	double  *dest = user_data;
	*dest = value;
	return 0;
}

avro_consumer_t *
avro_raw_double_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a double field if the writer is a double,
	 * or (TODO), a union containing a double.
	 */

	if (is_avro_double(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.double_value = avro_raw_double_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into double",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * float
 */

static int
avro_raw_float_value(avro_consumer_t *consumer, float value,
		     void *user_data)
{
	AVRO_UNUSED(consumer);
	float  *dest = user_data;
	*dest = value;
	return 0;
}

avro_consumer_t *
avro_raw_float_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a float field if the writer is a float,
	 * or (TODO), a union containing a float.
	 */

	if (is_avro_float(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.float_value = avro_raw_float_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into float",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * int
 */

static int
avro_raw_int_value(avro_consumer_t *consumer, int32_t value,
		   void *user_data)
{
	AVRO_UNUSED(consumer);
	int32_t  *dest = user_data;
	*dest = value;
	return 0;
}

avro_consumer_t *
avro_raw_int_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a int field if the writer is a int,
	 * or (TODO), a union containing a int.
	 */

	if (is_avro_int32(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.int_value = avro_raw_int_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into int",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * long
 */

static int
avro_raw_long_value(avro_consumer_t *consumer, int64_t value,
		    void *user_data)
{
	AVRO_UNUSED(consumer);
	int64_t  *dest = user_data;
	*dest = value;
	return 0;
}

avro_consumer_t *
avro_raw_long_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a long field if the writer is a long,
	 * or (TODO), a union containing a long.
	 */

	if (is_avro_int64(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.long_value = avro_raw_long_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into long",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * null
 */

static int
avro_raw_null_value(avro_consumer_t *consumer, void *user_data)
{
	AVRO_UNUSED(consumer);
	AVRO_UNUSED(user_data);
	return 0;
}

avro_consumer_t *
avro_raw_null_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a null field if the writer is a null,
	 * or (TODO), a union containing a null.
	 */

	if (is_avro_null(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.null_value = avro_raw_null_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into null",
		       avro_schema_type_name(wschema));
	return NULL;
}


/*-----------------------------------------------------------------------
 * string
 */

static int
avro_raw_string_value(avro_consumer_t *consumer,
		      const void *value, size_t value_len,
		      void *user_data)
{
	AVRO_UNUSED(consumer);
	avro_raw_string_t  *dest = user_data;
	avro_raw_string_set_length(dest, value, value_len);
	return 0;
}

avro_consumer_t *
avro_raw_string_resolver_new(avro_schema_t wschema)
{
	/*
	 * We can read into a string field if the writer is a string,
	 * or (TODO), a union containing a string.
	 */

	if (is_avro_string(wschema)) {
		avro_consumer_t  *consumer = avro_new(avro_consumer_t);
		memset(consumer, 0, sizeof(avro_consumer_t));
		consumer->callbacks.free = avro_raw_resolver_free;
		consumer->schema = avro_schema_incref(wschema);
		consumer->callbacks.string_value = avro_raw_string_value;
		return consumer;
	}

	avro_set_error("Cannot store %s into string",
		       avro_schema_type_name(wschema));
	return NULL;
}
