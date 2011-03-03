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

#include "avro/allocation.h"
#include "avro/consumer.h"
#include "st.h"


/**
 * Frees a consumer object, while ensuring that all of the consumers in
 * a graph of consumers is only freed once.
 */

static void
avro_consumer_free_cycles(avro_consumer_t *consumer, st_table *freeing)
{
	/*
	 * First check if we've already started freeing this consumer.
	 */

	if (st_lookup(freeing, (st_data_t) consumer, NULL)) {
		return;
	}

	/*
	 * Otherwise add this consumer to the freeing set, and then
	 * actually free the thing.
	 */

	st_insert(freeing, (st_data_t) consumer, (st_data_t) NULL);

	avro_schema_decref(consumer->schema);
	if (consumer->child_consumers) {
		unsigned int  i;
		for (i = 0; i < consumer->num_children; i++) {
			avro_consumer_t  *child = consumer->child_consumers[i];
			if (child) {
				avro_consumer_free_cycles(child, freeing);
			}
		}
		avro_free(consumer->child_consumers,
			  sizeof(avro_consumer_t *) * consumer->num_children);
	}

	if (consumer->callbacks.free) {
		consumer->callbacks.free(consumer);
	}
}


void
avro_consumer_free(avro_consumer_t *consumer)
{
	st_table  *freeing = st_init_numtable();
	avro_consumer_free_cycles(consumer, freeing);
	st_free_table(freeing);
}


void
avro_consumer_allocate_children(avro_consumer_t *consumer,
				size_t num_children)
{
	consumer->num_children = num_children;
	consumer->child_consumers =
	    avro_calloc(num_children, sizeof(avro_consumer_t *));
}
