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

#ifndef AVRO_SPECIFIC_H
#define AVRO_SPECIFIC_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdint.h>

#include <avro/data.h>

/*---------------------------------------------------------------------
 * “Raw” types
 */

typedef int  avro_raw_boolean_t;
typedef avro_raw_string_t  avro_raw_bytes_t;
typedef float  avro_raw_float_t;
typedef double  avro_raw_double_t;
typedef int32_t  avro_raw_int_t;
typedef int64_t  avro_raw_long_t;
typedef int  avro_raw_null_t;
/* Ha!  This one was well-named. */
/* typedef avro_raw_string_t  avro_raw_string_t; */


/*---------------------------------------------------------------------
 * “Raw” comparison
 */

int
avro_raw_boolean_equals(const avro_raw_boolean_t *val1,
			const avro_raw_boolean_t *val2);

int
avro_raw_bytes_equals(const avro_raw_bytes_t *val1,
		      const avro_raw_bytes_t *val2);

int
avro_raw_double_equals(const avro_raw_double_t *val1,
		       const avro_raw_double_t *val2);

int
avro_raw_float_equals(const avro_raw_float_t *val1,
		      const avro_raw_float_t *val2);

int
avro_raw_int_equals(const avro_raw_int_t *val1,
		    const avro_raw_int_t *val2);

int
avro_raw_long_equals(const avro_raw_long_t *val1,
		     const avro_raw_long_t *val2);

int
avro_raw_null_equals(const avro_raw_null_t *val1,
		     const avro_raw_null_t *val2);

/*
 * Also well-named!
int
avro_raw_string_equals(const avro_raw_string_t *val1,
		       const avro_raw_string_t *val2);
*/


CLOSE_EXTERN
#endif
