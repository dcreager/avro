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

#include <string.h>

#include "avro.h"
#include "avro/allocation.h"
#include "avro/consumer.h"
#include "avro/errors.h"
#include "avro/specific.h"
#include "avro_private.h"

int
avro_raw_boolean_consume(const avro_raw_boolean_t *value,
			 avro_consumer_t *consumer,
			 void *ud)
{
	return avro_consumer_call(consumer, boolean_value, *value, ud);
}

int
avro_raw_bytes_consume(const avro_raw_bytes_t *value,
		       avro_consumer_t *consumer,
		       void *ud)
{
	void  *buf = avro_raw_string_get(value);
	size_t  len = avro_raw_string_length(value);

	char  *bytes_copy = avro_malloc(len+1);
	if (!bytes_copy) {
		avro_set_error("Cannot allocate buffer for bytes value");
		return ENOMEM;
	}
	bytes_copy[len] = '\0';
	memcpy(bytes_copy, buf, len);

	return avro_consumer_call(consumer, bytes_value,
				  bytes_copy, len,
				  ud);
}

int
avro_raw_double_consume(const avro_raw_double_t *value,
			avro_consumer_t *consumer,
			void *ud)
{
	return avro_consumer_call(consumer, double_value, *value, ud);
}

int
avro_raw_float_consume(const avro_raw_float_t *value,
		       avro_consumer_t *consumer,
		       void *ud)
{
	return avro_consumer_call(consumer, float_value, *value, ud);
}

int
avro_raw_int_consume(const avro_raw_int_t *value,
		     avro_consumer_t *consumer,
		     void *ud)
{
	return avro_consumer_call(consumer, int_value, *value, ud);
}

int
avro_raw_long_consume(const avro_raw_long_t *value,
		      avro_consumer_t *consumer,
		      void *ud)
{
	return avro_consumer_call(consumer, long_value, *value, ud);
}

int
avro_raw_null_consume(const avro_raw_null_t *value,
		      avro_consumer_t *consumer,
		      void *ud)
{
	AVRO_UNUSED(value);
	return avro_consumer_call(consumer, null_value, ud);
}

int
avro_raw_string_consume(const avro_raw_string_t *value,
			avro_consumer_t *consumer,
			void *ud)
{
	void  *buf = avro_raw_string_get(value);
	size_t  len = avro_raw_string_length(value);

	char  *bytes_copy = avro_malloc(len);
	if (!bytes_copy) {
		avro_set_error("Cannot allocate buffer for string value");
		return ENOMEM;
	}
	memcpy(bytes_copy, buf, len);
	return avro_consumer_call(consumer, string_value,
				  bytes_copy, len,
				  ud);
}
