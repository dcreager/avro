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
#include "avro/specific.h"


static void
avro_specific_resolver_free(avro_consumer_t *consumer)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	avro_freet(avro_specific_resolver_t, resolver);
}


/*-----------------------------------------------------------------------
 * Helper macros
 */

#define AVRO_MEMOIZED(type_name) \
avro_specific_resolver_t * \
avro_raw_##type_name##_resolver_memoized(avro_memoize_t *mem, \
					 avro_schema_t wschema) \
{ \
	avro_specific_resolver_t  *result = NULL; \
	if (avro_memoize_get(mem, &avro_raw_##type_name##_memoize_key, \
			     wschema, (void **) &result)) { \
		return result; \
	} \
	\
	result = avro_raw_##type_name##_try(mem, wschema); \
	if (result) { return result; } \
	\
	result = avro_specific_resolve_writer_union \
	    (mem, &avro_raw_##type_name##_memoize_key, wschema, \
	     avro_raw_##type_name##_resolver_memoized); \
	if (result) { return result; } \
	\
	avro_set_error("Cannot store %s into " #type_name, \
		       avro_schema_type_name(wschema)); \
	return NULL; \
} \
\
avro_consumer_t * \
avro_raw_##type_name##_resolver_new(avro_schema_t wschema) \
{ \
	avro_memoize_t  mem; \
	avro_memoize_init(&mem); \
	avro_specific_resolver_t  *result = \
	    avro_raw_##type_name##_resolver_memoized(&mem, wschema); \
	avro_memoize_done(&mem); \
	return (result)? &result->parent: NULL; \
}


/*-----------------------------------------------------------------------
 * boolean
 */

static int
avro_raw_boolean_value(avro_consumer_t *consumer, int value,
		       void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	int  *dest = user_data;
	*dest = value;
	return 0;
}

static int  avro_raw_boolean_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_boolean_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	if (is_avro_boolean(wschema)) {
		avro_specific_resolver_t  *consumer = \
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.boolean_value =
		    avro_raw_boolean_value;
		avro_memoize_set(mem, &avro_raw_boolean_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(boolean)


/*-----------------------------------------------------------------------
 * bytes
 */

static int
avro_raw_bytes_value(avro_consumer_t *consumer,
		     const void *value, size_t value_len,
		     void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	avro_raw_string_t  *dest = user_data;
	avro_raw_string_set_length(dest, value, value_len);
	avro_free((void *) value, value_len+1);
	return 0;
}

static int  avro_raw_bytes_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_bytes_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	if (is_avro_bytes(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.bytes_value =
		    avro_raw_bytes_value;
		avro_memoize_set(mem, &avro_raw_bytes_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(bytes)


/*-----------------------------------------------------------------------
 * double
 */

static int
avro_raw_double_value(avro_consumer_t *consumer, double value,
		      void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	double  *dest = user_data;
	*dest = value;
	return 0;
}

static int  avro_raw_double_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_double_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	if (is_avro_double(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.double_value =
		    avro_raw_double_value;
		avro_memoize_set(mem, &avro_raw_double_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(double)


/*-----------------------------------------------------------------------
 * float
 */

static int
avro_raw_float_value(avro_consumer_t *consumer, float value,
		     void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	float  *dest = user_data;
	*dest = value;
	return 0;
}

static int  avro_raw_float_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_float_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	if (is_avro_float(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.float_value =
		    avro_raw_float_value;
		avro_memoize_set(mem, &avro_raw_float_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(float)


/*-----------------------------------------------------------------------
 * int
 */

static int
avro_raw_int_value(avro_consumer_t *consumer, int32_t value,
		   void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	int32_t  *dest = user_data;
	*dest = value;
	return 0;
}

static int  avro_raw_int_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_int_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	if (is_avro_int32(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.int_value =
		    avro_raw_int_value;
		avro_memoize_set(mem, &avro_raw_int_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(int)


/*-----------------------------------------------------------------------
 * long
 */

static int
avro_raw_long_value(avro_consumer_t *consumer, int64_t value,
		    void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	int64_t  *dest = user_data;
	*dest = value;
	return 0;
}

static int  avro_raw_long_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_long_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	/*
	 * We can read into a long field if the writer is a long,
	 * or (TODO), a union containing a long.
	 */

	if (is_avro_int64(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.long_value =
		    avro_raw_long_value;
		avro_memoize_set(mem, &avro_raw_long_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(long)


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

static int  avro_raw_null_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_null_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	/*
	 * We can read into a null field if the writer is a null,
	 * or (TODO), a union containing a null.
	 */

	if (is_avro_null(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.null_value =
		    avro_raw_null_value;
		avro_memoize_set(mem, &avro_raw_null_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(null)


/*-----------------------------------------------------------------------
 * string
 */

static int
avro_raw_string_value(avro_consumer_t *consumer,
		      const void *value, size_t value_len,
		      void *user_data)
{
	avro_specific_resolver_t  *resolver =
	    (avro_specific_resolver_t *) consumer;
	if (resolver->branch_selector) {
		user_data = resolver->branch_selector(user_data);
	}
	avro_raw_string_t  *dest = user_data;
	avro_raw_string_set_length(dest, value, value_len);
	avro_free((void *) value, value_len);
	return 0;
}

static int  avro_raw_string_memoize_key = 0;

static avro_specific_resolver_t *
avro_raw_string_try(avro_memoize_t *mem, avro_schema_t wschema)
{
	/*
	 * We can read into a string field if the writer is a string,
	 * or (TODO), a union containing a string.
	 */

	if (is_avro_string(wschema)) {
		avro_specific_resolver_t  *consumer =
		    avro_new(avro_specific_resolver_t);
		memset(consumer, 0, sizeof(avro_specific_resolver_t));
		consumer->parent.callbacks.free = avro_specific_resolver_free;
		consumer->parent.schema = avro_schema_incref(wschema);
		consumer->parent.callbacks.string_value =
		    avro_raw_string_value;
		avro_memoize_set(mem, &avro_raw_string_memoize_key,
				 wschema, consumer);
		return consumer;
	}

	return NULL;
}

AVRO_MEMOIZED(string)


/*-----------------------------------------------------------------------
 * writer unions
 */

static int
avro_specific_writer_union_branch(avro_consumer_t *consumer,
				  unsigned int discriminant,
				  avro_consumer_t **branch_consumer,
				  void **branch_user_data,
				  void *user_data)
{
	avro_consumer_t  *branch = consumer->child_consumers[discriminant];
	if (!branch) {
		avro_set_error("Writer union branch %u is incompatible "
			       "with reader schema", discriminant);
		return EINVAL;
	}

	*branch_consumer = branch;
	*branch_user_data = user_data;
	return 0;
}

avro_specific_resolver_t *
avro_specific_resolve_writer_union(avro_memoize_t *mem,
				   void *rschema_key,
				   avro_schema_t wschema,
				   avro_specific_try_branch_func_t try_branch)
{
	if (!is_avro_union(wschema)) {
		return NULL;
	}

	avro_specific_resolver_t  *result = avro_new(avro_specific_resolver_t);
	memset(result, 0, sizeof(avro_specific_resolver_t));
	result->parent.callbacks.free = avro_specific_resolver_free;
	result->parent.schema = avro_schema_incref(wschema);
	if (mem) {
		avro_memoize_set(mem, rschema_key, wschema, result);
	}

	size_t  num_branches = avro_schema_union_size(wschema);
	avro_consumer_allocate_children(&result->parent, num_branches);

	int  some_branch_compatible = 0;
	size_t  i;
	for (i = 0; i < num_branches; i++) {
		avro_schema_t  wbranch = avro_schema_union_branch(wschema, i);

		avro_specific_resolver_t  *branch_resolver =
		    try_branch(mem, wbranch);
		if (branch_resolver) {
			some_branch_compatible = 1;
		}

		result->parent.child_consumers[i] = &branch_resolver->parent;
	}

	if (!some_branch_compatible) {
		avro_consumer_free(&result->parent);
		avro_set_error("No branches in writer union are compatible "
			       "with reader schema");
		avro_memoize_delete(mem, rschema_key, wschema);
		return NULL;
	}

	result->parent.callbacks.union_branch =
		avro_specific_writer_union_branch;
	return result;
}
