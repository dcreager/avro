#include <avro.h>
#include <libcork/core.h>
#include <libcork/core/checkers.h>

/**
 * Let's say your producing some Avro data, and moreover, that you're
 * adding this functionality to an existing application.  That means
 * that you've probably already got some C type (or family of types) to
 * model and store your data.
 */

struct person {
    const char  *first_name;
    const char  *last_name;
    int  age;
    size_t  child_count;
    struct person  **children;
};

/**
 * Given this data type, you might decide to output Avro data that
 * conforms to the following schema:
 *
 *     {
 *       "type": "record",
 *       "name": "person",
 *       "fields": [
 *         {"name": "first_name", "type": "string"},
 *         {"name": "last_name", "type": "string"},
 *         {"name": "age", "type": "int"},
 *         {"name": "children", "type":
 *          {"type": "array", "items": "person"}}
 *       ]
 *     }
 *
 * Out of the box, the only way to create Avro data of this schema is to
 * use the Avro library's _generic value implementation_.  This involves
 * a number of steps.  First, you have to get the schema yourself into
 * your C code somehow.  Currently, the easiest way to do this is to
 * embed the JSON representation as a string-literal.
 *
 * It's annoying, yes, and eventually it would be nice to have a tool
 * that auto-escapes for us, but then again, in vi, `:s/"/\\"/g` escapes
 * all of the JSON quotes, `I"` inserts a quote at the beginning of the
 * line, `A"` inserts one at the end, and `.` is your magic “repeat the
 * last command” button.  So the manual escaping only takes 1 minute
 * once you get the hang of it.
 */

static const char  PERSON_SCHEMA[] =
"{"
"  \"type\": \"record\","
"  \"name\": \"person\","
"  \"fields\": ["
"    {\"name\": \"first_name\", \"type\": \"string\"},"
"    {\"name\": \"last_name\", \"type\": \"string\"},"
"    {\"name\": \"age\", \"type\": \"int\"},"
"    {\"name\": \"children\", \"type\": "
"     {\"type\": \"array\", \"items\": \"person\"}}"
"  ]"
"}"
;

avro_schema_t
create_person_schema(void)
{
    avro_schema_error_t  error;
    avro_schema_t  schema = NULL;
    avro_schema_from_json
        (PERSON_SCHEMA, sizeof(PERSON_SCHEMA)-1, &schema, &error);
    return schema;
}

/**
 * Next, you need to instantiate an _Avro value instance_ that you'll
 * copy the data into.  This also involves instantiating a particular
 * _value implementation_; as mentioned above, the only implementation
 * that you get out of the box is the _generic value implementation_.
 *
 * (Note that we're using the [error-checking
 * macros](http://readthedocs.org/docs/libcork/en/latest/errors/#error-checking-macros)
 * from [libcork](https://github.com/redjack/libcork).)
 */

int
create_person_value(avro_value_t *dest)
{
    avro_schema_t  schema = NULL;
    avro_value_iface_t  *iface = NULL;

    ep_check(schema = create_person_schema());
    ep_check(iface = avro_generic_class_from_schema(schema));
    ei_check(avro_generic_value_new(iface, dest));
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    return 0;

error:
    if (iface != NULL) {
        avro_value_iface_decref(iface);
    }

    if (schema != NULL) {
        avro_schema_decref(schema);
    }

    return -1;
}


/**
 * Now, given an Avro value instance, and an instance of our
 * application's data type, we can define a function that copies the
 * data from the latter to the former.
 *
 * Not too bad, right?  You can then write the `avro_value_t` instance
 * into an Avro data file, for example, using
 * `avro_file_writer_append_value`.
 *
 * One thing to note about the `fill_person_value` function is that it
 * makes a full copy of all of the data in the `src` record.  This can
 * be good and bad; it's good because the `avro_value_t` instance can
 * outlive the original `struct person` instance, if needed.  It can be
 * bad, though, that we're doing all of this memory copying.
 */

int
fill_person_value(avro_value_t *dest, const struct person *src)
{
    avro_value_t  child;
    size_t  i;
    /* Field 0: first_name */
    rii_check(avro_value_get_by_index(dest, 0, &child, NULL));
    rii_check(avro_value_set_string(&child, src->first_name));
    /* Field 1: last_name */
    rii_check(avro_value_get_by_index(dest, 1, &child, NULL));
    rii_check(avro_value_set_string(&child, src->last_name));
    /* Field 2: age */
    rii_check(avro_value_get_by_index(dest, 2, &child, NULL));
    rii_check(avro_value_set_int(&child, src->age));
    /* Field 3: children */
    rii_check(avro_value_get_by_index(dest, 3, &child, NULL));
    rii_check(avro_value_reset(&child));
    for (i = 0; i < src->child_count; i++) {
        avro_value_t  element;
        rii_check(avro_value_append(&child, &element, NULL));
        rii_check(fill_person_value(&element, src->children[i]));
    }
    return 0;
}

/**
 * If you can **guarantee** that the `struct person` will around for the
 * entire lifetime of the `avro_value_t`, then you can save some cycles
 * by _wrapping_ the names instead of _copying_ them.  (Boolean and
 * numeric fields are always copied, since the cost of copying the value
 * is the same as the cost of copying a pointer to the value.)
 *
 * This is the best we can do, performance-wise, using the generic value
 * implementation.  There's no copying of large binary buffers, though
 * we do still have the overhead of copying pointers to the buffers, the
 * overhead of copying the numeric fields, and most importantly, the
 * overhead of all of those `avro_value_*` function calls.  With only
 * slightly more pain, we can do better than this by creating a
 * [custom Avro value class](custom-value-class.html) for our
 * application data type.
 */

int
fill_person_value_wrapped(avro_value_t *dest, const struct person *src)
{
    avro_value_t  child;
    avro_wrapped_buffer_t  buf;
    size_t  i;
    /* Field 0: first_name */
    rii_check(avro_value_get_by_index(dest, 0, &child, NULL));
    rii_check(avro_wrapped_buffer_new_string(&buf, src->first_name));
    rii_check(avro_value_give_string(&child, &buf));
    /* Field 1: last_name */
    rii_check(avro_value_get_by_index(dest, 1, &child, NULL));
    rii_check(avro_wrapped_buffer_new_string(&buf, src->last_name));
    rii_check(avro_value_give_string(&child, &buf));
    /* Field 2: age */
    rii_check(avro_value_get_by_index(dest, 2, &child, NULL));
    rii_check(avro_value_set_int(&child, src->age));
    /* Field 3: children */
    rii_check(avro_value_get_by_index(dest, 3, &child, NULL));
    rii_check(avro_value_reset(&child));
    for (i = 0; i < src->child_count; i++) {
        avro_value_t  element;
        rii_check(avro_value_append(&child, &element, NULL));
        rii_check(fill_person_value(&element, src->children[i]));
    }
    return 0;
}
