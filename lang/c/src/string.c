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

#include <string.h>

#include "avro/data.h"
#include "avro/allocation.h"
#include "avro/errors.h"


void avro_raw_string_init(avro_raw_string_t *str)
{
	memset(str, 0, sizeof(avro_raw_string_t));
}


static void
avro_raw_string_free_buf(avro_raw_string_t *str)
{
	if (str->free) {
		str->free(str->buf, str->allocated_size);
		str->free = NULL;
	}
	str->buf = NULL;
	str->allocated_size = 0;
}


void
avro_raw_string_clear(avro_raw_string_t *str)
{
	/*
	 * If the string's buffer is one that we control, then we don't
	 * free it; that lets us reuse the storage on the next call to
	 * avro_raw_string_set[_length].
	 */

	if (!str->our_buf) {
		avro_raw_string_free_buf(str);
	}

	str->size = 0;
}


void avro_raw_string_done(avro_raw_string_t *str)
{
	avro_raw_string_free_buf(str);
	memset(str, 0, sizeof(avro_raw_string_t));
}


/**
 * Makes sure that the string's buffer is one that we allocated
 * ourselves, and that the buffer is big enough to hold a string of the
 * given length.
 */

static void
avro_raw_string_ensure_buf(avro_raw_string_t *str, size_t length)
{
	if (!str->our_buf) {
		avro_raw_string_free_buf(str);
	}

	if (str->allocated_size == 0) {
		str->buf = avro_malloc(length);
		str->allocated_size = length;
		str->free = avro_alloc_free;
	}

	else if (length > str->allocated_size) {
		size_t  new_size = str->allocated_size * 2;
		str->buf = avro_realloc(str->buf, str->allocated_size, new_size);
		str->allocated_size = new_size;
	}
}


void avro_raw_string_set_length(avro_raw_string_t *str,
				const void *src,
				size_t length)
{
	avro_raw_string_ensure_buf(str, length+1);
	memcpy(str->buf, src, length);
	((char *) str->buf)[length] = '\0';
	str->size = length;
}


void avro_raw_string_set(avro_raw_string_t *str, const char *src)
{
	size_t  length = strlen(src);
	avro_raw_string_ensure_buf(str, length+1);
	memcpy(str->buf, src, length+1);
	str->size = length+1;
}


void avro_raw_string_give_length(avro_raw_string_t *str,
				 void *src,
				 size_t length,
				 avro_free_func_t free)
{
	avro_raw_string_free_buf(str);
	str->buf = src;
	str->allocated_size = length;
	str->size = length;
	str->free = free;
	str->our_buf = 0;
}


void avro_raw_string_give(avro_raw_string_t *str,
			  char *src,
			  avro_free_func_t free)
{
	size_t  length = strlen(src);
	avro_raw_string_give_length(str, src, length+1, free);
}


int avro_raw_string_equals(const avro_raw_string_t *str1,
			   const avro_raw_string_t *str2)
{
	if (str1 == str2) {
		return 1;
	}

	if (!str1 || !str2) {
		return 0;
	}
	
	if (str1->size != str2->size) {
		return 0;
	}

	return (memcmp(str1->buf, str2->buf, str1->size) == 0);
}
