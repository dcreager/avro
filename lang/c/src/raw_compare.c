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
#include "avro_private.h"
#include "avro/specific.h"

int
avro_raw_boolean_equals(const avro_raw_boolean_t *val1,
			const avro_raw_boolean_t *val2)
{
	if (val1 == val2) {
		return 1;
	}
	if (!val1 || !val2) {
		return 0;
	}
	return *val1 == *val2;
}

int
avro_raw_bytes_equals(const avro_raw_bytes_t *val1,
		      const avro_raw_bytes_t *val2)
{
	return avro_raw_string_equals(val1, val2);
}

int
avro_raw_double_equals(const avro_raw_double_t *val1,
		       const avro_raw_double_t *val2)
{
	if (val1 == val2) {
		return 1;
	}
	if (!val1 || !val2) {
		return 0;
	}
	return *val1 == *val2;
}

int
avro_raw_float_equals(const avro_raw_float_t *val1,
		      const avro_raw_float_t *val2)
{
	if (val1 == val2) {
		return 1;
	}
	if (!val1 || !val2) {
		return 0;
	}
	return *val1 == *val2;
}

int
avro_raw_int_equals(const avro_raw_int_t *val1,
		    const avro_raw_int_t *val2)
{
	if (val1 == val2) {
		return 1;
	}
	if (!val1 || !val2) {
		return 0;
	}
	return *val1 == *val2;
}

int
avro_raw_long_equals(const avro_raw_long_t *val1,
		     const avro_raw_long_t *val2)
{
	if (val1 == val2) {
		return 1;
	}
	if (!val1 || !val2) {
		return 0;
	}
	return *val1 == *val2;
}

int
avro_raw_null_equals(const avro_raw_null_t *val1,
		     const avro_raw_null_t *val2)
{
	/*
	 * We don't have to actually compare the values for NULLs, since
	 * they're just dummy placeholders.  We just need to worry about
	 * the pointers.
	 */

	return !val1 == !val2;
}
