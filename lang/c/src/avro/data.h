/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef AVRO_DATA_H
#define AVRO_DATA_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdlib.h>

/*
 * This file defines some helper data structures that are used within
 * Avro, and in the schema-specific types created by avrocc.
 */


/**
 * A resizeable array of fixed-size elements.
 */

typedef struct avro_raw_array {
	size_t  element_size;
	size_t  element_count;
	size_t  allocated_size;
	void  *data;
} avro_raw_array_t;

/**
 * Initializes a new avro_raw_array_t that you've allocated yourself.
 */

void avro_raw_array_init(avro_raw_array_t *array, size_t element_size);

/**
 * Finalizes an avro_raw_array_t.
 */

void avro_raw_array_done(avro_raw_array_t *array);

/**
 * Returns the number of elements in an avro_raw_array_t.
 */

#define avro_raw_array_size(array) ((array)->element_count)

/**
 * Returns the given element of an avro_raw_array_t, using element_type
 * as the type of the elements.  The result is *not* a pointer to the
 * element; you get the element itself.
 */

#define avro_raw_array_get(array, element_type, index) \
	(((element_type *) (array)->data)[index])

/**
 * Appends a new element to an avro_raw_array_t, expanding it if
 * necessary.  Returns a pointer to the new element, or NULL if we
 * needed to reallocate the array and couldn't.
 */

void *avro_raw_array_append(avro_raw_array_t *array);


CLOSE_EXTERN
#endif
