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


/*---------------------------------------------------------------------
 * Arrays
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


/*---------------------------------------------------------------------
 * Maps
 */

/**
 * A string-indexed map of fixed-size elements.
 */

typedef struct avro_raw_map {
	avro_raw_array_t  elements;
	void  *indices_by_key;
} avro_raw_map_t;

/**
 * Initializes a new avro_raw_map_t that you've allocated yourself.
 */

void avro_raw_map_init(avro_raw_map_t *map, size_t element_size);

/**
 * Finalizes an avro_raw_map_t.
 */

void avro_raw_map_done(avro_raw_map_t *map);

/**
 * Returns the number of elements in an avro_raw_map_t.
 */

#define avro_raw_map_size(map)  avro_raw_array_size(&((map)->elements))

/**
 * Returns the element of an avro_raw_map_t with the given numeric
 * index.  The indexes are assigned based on the order that the elements
 * are added to the map.
 */

#define avro_raw_map_get_by_index(map, element_type, index) \
	avro_raw_array_get(&((map)->elements), element_type, index)

/**
 * Returns the element of an avro_raw_map_t with the given string key.
 * If the given element doesn't exist, returns NULL.  If @ref index
 * isn't NULL, it will be filled in with the index of the element.
 */

void *avro_raw_map_get(avro_raw_map_t *map, const char *key,
		       unsigned int *index);

/**
 * Retrieves the element of an avro_raw_map_t with the given string key,
 * creating it if necessary.  A pointer to the element is placed into
 * @ref element.  If @ref index isn't NULL, it will be filled in with
 * the index of the element.  We return 1 if the element is new; 0 if
 * it's not, and a negative error code if there was some problem.
 */

int avro_raw_map_get_or_create(avro_raw_map_t *map, const char *key,
			       void **element, unsigned int *index);


/*---------------------------------------------------------------------
 * Strings
 */

/**
 * A function used to free the contents of an avro_raw_string_t
 * instance.
 */

typedef void
(*avro_free_func_t)(void *ptr, size_t sz);

/**
 * An avro_free_func_t that frees the buffer using the custom allocator
 * provided to avro_set_allocator.
 */

void
avro_alloc_free(void *ptr, size_t sz);

/**
 * A resizable buffer for storing strings and bytes values.
 */

typedef struct avro_raw_string {
	size_t  size;
	size_t  allocated_size;
	void  *buf;
	avro_free_func_t  free;
	int  our_buf;
} avro_raw_string_t;

/**
 * Initializes an avro_raw_string_t that you've allocated yourself.
 */

void avro_raw_string_init(avro_raw_string_t *str);

/**
 * Finalizes an avro_raw_string_t.
 */

void avro_raw_string_done(avro_raw_string_t *str);

/**
 * Returns the length of the data stored in an avro_raw_string_t.  If
 * the buffer contains a C string, this length includes the NUL
 * terminator.
 */

#define avro_raw_string_length(str)  ((str)->size)

/**
 * Returns a pointer to the data stored in an avro_raw_string_t.
 */

#define avro_raw_string_get(str)  ((str)->buf)

/**
 * Fills an avro_raw_string_t with a copy of the given buffer.
 */

void avro_raw_string_set_length(avro_raw_string_t *str,
				const void *src,
				size_t length);

/**
 * Fills an avro_raw_string_t with a copy of the given C string.
 */

void avro_raw_string_set(avro_raw_string_t *str, const char *src);

/**
 * Gives control of a buffer to an avro_raw_string_t.
 */

void avro_raw_string_give_length(avro_raw_string_t *str,
				 void *src,
				 size_t length,
				 avro_free_func_t free);

/**
 * Gives control of a C string to an avro_raw_string_t.
 */

void avro_raw_string_give(avro_raw_string_t *str,
			  char *src,
			  avro_free_func_t free);

/**
 * Clears an avro_raw_string_t.
 */

void avro_raw_string_clear(avro_raw_string_t *str);


CLOSE_EXTERN
#endif
