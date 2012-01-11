#include <string.h>
#include <avro.h>

/**
 * This example assumes that you already know how to create Avro data
 * using the [generic value implementation](avro-values.html).  This
 * page describes how to squeeze out a bit more performance by writing a
 * custom value implementation.  We're going to reuse a couple of the
 * functions, and the application data type, defined there.
 *
 * We're also going to use the same Avro schema:
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
 */

struct person {
    const char  *first_name;
    const char  *last_name;
    int  age;
    size_t  child_count;
    struct person  **children;
};

avro_schema_t
create_person_schema(void);

/**
 * ## Preliminaries
 *
 * Each value implementation is defined by an instance of the
 * `avro_value_iface_t` struct.  This type contains a slew of function
 * pointers, which provide the actual implementations of most of the
 * `avro_value_*` functions that you call to interact with an Avro
 * value.  The interface struct is rather large, so we're not going to
 * reproduce it here, but you can find its definition in
 * `avro/value.h`.
 *
 * Each value consists of two pointers: a pointer to a value
 * implementation, and a `self` pointer.  Each value implementation is
 * free to interpret the `self` pointer however they wish.  We'll use it
 * as a pointer to an existing `struct person` object.
 */

/**
 * Our goal is to write a value implementation that accesses a `struct
 * person` directly when you call the "getter" functions —
 * `avro_value_get_*`.  We'll end up with one `avro_value_iface_t` for
 * the `"person"` record type, as one as one for each of the fields in
 * the record.  The implementation structs themselves are pretty simple;
 * we define the `get_type` and `get_schema` methods for each of them,
 * and whichever getter methods are appropriate for that
 * implementation's schema type.
 */

static avro_value_iface_t  person_iface;
static avro_value_iface_t  person_first_name_iface;
static avro_value_iface_t  person_last_name_iface;
static avro_value_iface_t  person_age_iface;
static avro_value_iface_t  person_children_iface;


/**
 * ## Initializing a value
 *
 * We can go ahead and define the function that initializes an
 * `avro_value_t` so that it wraps a `struct person` object.
 */

int
wrap_person(avro_value_t *dest, struct person *src)
{
    dest->iface = &person_iface;
    dest->self = src;
    return 0;
}


/**
 * ## A quick note about schemas
 *
 * Each of the value implementations needs to provide the `get_schema`
 * method, which returns the Avro schema represented an Avro value that
 * uses that implementation.  This method is called fairly often, so we
 * want to cache the schema value across calls.
 *
 * (Note that this function is **not** thread-safe.)
 */

static avro_schema_t  CACHED_PERSON_SCHEMA = NULL;

static avro_schema_t
get_cached_person_schema(void)
{
    if (CACHED_PERSON_SCHEMA == NULL) {
        CACHED_PERSON_SCHEMA = create_person_schema();
    }
    return CACHED_PERSON_SCHEMA;
}


/**
 * ## Top-level record
 *
 * Each of our value implementations needs `get_type` and `get_schema`:
 */

static avro_type_t
person_get_type(const avro_value_iface_t *iface, const void *vself)
{
    return AVRO_RECORD;
}

static avro_schema_t
person_get_schema(const avro_value_iface_t *iface, const void *vself)
{
    return get_cached_person_schema();
}

/**
 * For a `record` schema, we need to define the `get_size`,
 * `get_by_index`, and `get_by_name` methods.
 */

static int
person_get_size(const avro_value_iface_t *iface, const void *self,
                size_t *size)
{
    /* There are four fields in the person schema */
    *size = 4;
    return 0;
}

static int
person_get_by_index(const avro_value_iface_t *iface, const void *self,
                    size_t index, avro_value_t *dest, const char **name)
{
    switch (index) {
        case 0:
            dest->iface = &person_first_name_iface;
            dest->self = (void *) self;
            if (name != NULL) { *name = "first_name"; }
            return 0;

        case 1:
            dest->iface = &person_last_name_iface;
            dest->self = (void *) self;
            if (name != NULL) { *name = "last_name"; }
            return 0;

        case 2:
            dest->iface = &person_age_iface;
            dest->self = (void *) self;
            if (name != NULL) { *name = "age"; }
            return 0;

        case 3:
            dest->iface = &person_children_iface;
            dest->self = (void *) self;
            if (name != NULL) { *name = "children"; }
            return 0;

        default:
            avro_set_error("Invalid person field index %zu", index);
            return -1;
    }
}

static int
person_get_by_name(const avro_value_iface_t *iface, const void *self,
                   const char *name, avro_value_t *dest, size_t *index)
{
    if (strcmp(name, "first_name") == 0) {
        dest->iface = &person_first_name_iface;
        dest->self = (void *) self;
        if (index != NULL) { *index = 0; }
        return 0;
    }

    if (strcmp(name, "last_name") == 0) {
        dest->iface = &person_last_name_iface;
        dest->self = (void *) self;
        if (index != NULL) { *index = 1; }
        return 0;
    }

    if (strcmp(name, "age") == 0) {
        dest->iface = &person_age_iface;
        dest->self = (void *) self;
        if (index != NULL) { *index = 2; }
        return 0;
    }

    if (strcmp(name, "children") == 0) {
        dest->iface = &person_children_iface;
        dest->self = (void *) self;
        if (index != NULL) { *index = 3; }
        return 0;
    }

    avro_set_error("Invalid person field name %s", name);
    return -1;
}


/**
 * ## First name
 *
 * Each of our value implementations needs `get_type` and `get_schema`:
 */

static avro_type_t
person_first_name_get_type(const avro_value_iface_t *iface,
                           const void *vself)
{
    return AVRO_STRING;
}

static avro_schema_t
person_first_name_get_schema(const avro_value_iface_t *iface,
                             const void *vself)
{
    avro_schema_t  person = get_cached_person_schema();
    return avro_schema_record_field_get_by_index(person, 0);
}

/**
 * For a `string` schema, we need to define the `get_string` and
 * `grab_string` methods.
 */

static int
person_first_name_get_string(const avro_value_iface_t *iface,
                             const void *vself, const char **str,
                             size_t *size)
{
    const struct person  *self = vself;
    *str = self->first_name;
    if (size != NULL) {
        /* size must include NUL terminator */
        *size = strlen(self->first_name) + 1;
    }
    return 0;
}

static int
person_first_name_grab_string(const avro_value_iface_t *iface,
                              const void *vself,
                              avro_wrapped_buffer_t *dest)
{
    const struct person  *self = vself;
    return avro_wrapped_buffer_new_string(dest, self->first_name);
}


/**
 * ## Last name
 *
 * Each of our value implementations needs `get_type` and `get_schema`:
 */

static avro_type_t
person_last_name_get_type(const avro_value_iface_t *iface,
                          const void *vself)
{
    return AVRO_STRING;
}

static avro_schema_t
person_last_name_get_schema(const avro_value_iface_t *iface,
                            const void *vself)
{
    avro_schema_t  person = get_cached_person_schema();
    return avro_schema_record_field_get_by_index(person, 1);
}

/**
 * For a `string` schema, we need to define the `get_string` and
 * `grab_string` methods.
 */

static int
person_last_name_get_string(const avro_value_iface_t *iface,
                            const void *vself, const char **str,
                            size_t *size)
{
    const struct person  *self = vself;
    *str = self->last_name;
    if (size != NULL) {
        /* size must include NUL terminator */
        *size = strlen(self->last_name) + 1;
    }
    return 0;
}

static int
person_last_name_grab_string(const avro_value_iface_t *iface,
                             const void *vself,
                             avro_wrapped_buffer_t *dest)
{
    const struct person  *self = vself;
    return avro_wrapped_buffer_new_string(dest, self->last_name);
}


/**
 * ## Age
 *
 * Each of our value implementations needs `get_type` and `get_schema`:
 */

static avro_type_t
person_age_get_type(const avro_value_iface_t *iface,
                    const void *vself)
{
    return AVRO_INT32;
}

static avro_schema_t
person_age_get_schema(const avro_value_iface_t *iface,
                      const void *vself)
{
    avro_schema_t  person = get_cached_person_schema();
    return avro_schema_record_field_get_by_index(person, 2);
}

/**
 * For an `int` schema, we need to define the `get_int` method.
 */

static int
person_age_get_int(const avro_value_iface_t *iface,
                   const void *vself, int32_t *out)
{
    const struct person  *self = vself;
    *out = self->age;
    return 0;
}


/**
 * ## Children
 *
 * Each of our value implementations needs `get_type` and `get_schema`:
 */

static avro_type_t
person_children_get_type(const avro_value_iface_t *iface,
                         const void *vself)
{
    return AVRO_ARRAY;
}

static avro_schema_t
person_children_get_schema(const avro_value_iface_t *iface,
                           const void *vself)
{
    avro_schema_t  person = get_cached_person_schema();
    return avro_schema_record_field_get_by_index(person, 3);
}

/**
 * For an `array` schema, we need to define the `get_size` and
 * `get_by_index` method.
 */

static int
person_children_get_size(const avro_value_iface_t *iface,
                         const void *vself, size_t *size)
{
    const struct person  *self = vself;
    *size = self->child_count;
    return 0;
}

static int
person_children_get_by_index(const avro_value_iface_t *iface,
                             const void *vself, size_t index,
                             avro_value_t *dest, const char **name)
{
    const struct person  *self = vself;
    dest->iface = &person_iface;
    dest->self = (void *) self->children[index];
    return 0;
}


/**
 * ## Implementation structs
 *
 * With all of the individual methods defined, our last step is to
 * define each of the value implementation structs.
 *
 * Yes, this is pretty repetitive.  We should probably add some helper
 * macros to the Avro library to simplify this.
 */

static avro_value_iface_t  person_iface = {
    NULL, /* incref_iface */
    NULL, /* decref_iface */
    NULL, /* incref */
    NULL, /* decref */
    NULL, /* reset */
    person_get_type,
    person_get_schema,
    NULL, /* get_boolean */
    NULL, /* get_bytes */
    NULL, /* grab_bytes */
    NULL, /* get_double */
    NULL, /* get_float */
    NULL, /* get_int */
    NULL, /* get_long */
    NULL, /* get_null */
    NULL, /* get_string */
    NULL, /* grab_string */
    NULL, /* get_enum */
    NULL, /* get_fixed */
    NULL, /* grab_fixed */
    NULL, /* set_boolean */
    NULL, /* set_bytes */
    NULL, /* give_bytes */
    NULL, /* set_double */
    NULL, /* set_float */
    NULL, /* set_int */
    NULL, /* set_long */
    NULL, /* set_null */
    NULL, /* set_string */
    NULL, /* set_string_len */
    NULL, /* give_string_len */
    NULL, /* set_enum */
    NULL, /* set_fixed */
    NULL, /* give_fixed */
    person_get_size,
    person_get_by_index,
    person_get_by_name,
    NULL, /* get_discriminant */
    NULL, /* get_current_branch */
    NULL, /* append */
    NULL, /* add */
    NULL  /* set_branch */
};

static avro_value_iface_t  person_first_name_iface = {
    NULL, /* incref_iface */
    NULL, /* decref_iface */
    NULL, /* incref */
    NULL, /* decref */
    NULL, /* reset */
    person_first_name_get_type,
    person_first_name_get_schema,
    NULL, /* get_boolean */
    NULL, /* get_bytes */
    NULL, /* grab_bytes */
    NULL, /* get_double */
    NULL, /* get_float */
    NULL, /* get_int */
    NULL, /* get_long */
    NULL, /* get_null */
    person_first_name_get_string,
    person_first_name_grab_string,
    NULL, /* get_enum */
    NULL, /* get_fixed */
    NULL, /* grab_fixed */
    NULL, /* set_boolean */
    NULL, /* set_bytes */
    NULL, /* give_bytes */
    NULL, /* set_double */
    NULL, /* set_float */
    NULL, /* set_int */
    NULL, /* set_long */
    NULL, /* set_null */
    NULL, /* set_string */
    NULL, /* set_string_len */
    NULL, /* give_string_len */
    NULL, /* set_enum */
    NULL, /* set_fixed */
    NULL, /* give_fixed */
    NULL, /* get_size */
    NULL, /* get_by_index */
    NULL, /* get_by_name */
    NULL, /* get_discriminant */
    NULL, /* get_current_branch */
    NULL, /* append */
    NULL, /* add */
    NULL  /* set_branch */
};

static avro_value_iface_t  person_last_name_iface = {
    NULL, /* incref_iface */
    NULL, /* decref_iface */
    NULL, /* incref */
    NULL, /* decref */
    NULL, /* reset */
    person_last_name_get_type,
    person_last_name_get_schema,
    NULL, /* get_boolean */
    NULL, /* get_bytes */
    NULL, /* grab_bytes */
    NULL, /* get_double */
    NULL, /* get_float */
    NULL, /* get_int */
    NULL, /* get_long */
    NULL, /* get_null */
    person_last_name_get_string,
    person_last_name_grab_string,
    NULL, /* get_enum */
    NULL, /* get_fixed */
    NULL, /* grab_fixed */
    NULL, /* set_boolean */
    NULL, /* set_bytes */
    NULL, /* give_bytes */
    NULL, /* set_double */
    NULL, /* set_float */
    NULL, /* set_int */
    NULL, /* set_long */
    NULL, /* set_null */
    NULL, /* set_string */
    NULL, /* set_string_len */
    NULL, /* give_string_len */
    NULL, /* set_enum */
    NULL, /* set_fixed */
    NULL, /* give_fixed */
    NULL, /* get_size */
    NULL, /* get_by_index */
    NULL, /* get_by_name */
    NULL, /* get_discriminant */
    NULL, /* get_current_branch */
    NULL, /* append */
    NULL, /* add */
    NULL  /* set_branch */
};

static avro_value_iface_t  person_age_iface = {
    NULL, /* incref_iface */
    NULL, /* decref_iface */
    NULL, /* incref */
    NULL, /* decref */
    NULL, /* reset */
    person_age_get_type,
    person_age_get_schema,
    NULL, /* get_boolean */
    NULL, /* get_bytes */
    NULL, /* grab_bytes */
    NULL, /* get_double */
    NULL, /* get_float */
    person_age_get_int,
    NULL, /* get_long */
    NULL, /* get_null */
    NULL, /* get_string */
    NULL, /* grab_string */
    NULL, /* get_enum */
    NULL, /* get_fixed */
    NULL, /* grab_fixed */
    NULL, /* set_boolean */
    NULL, /* set_bytes */
    NULL, /* give_bytes */
    NULL, /* set_double */
    NULL, /* set_float */
    NULL, /* set_int */
    NULL, /* set_long */
    NULL, /* set_null */
    NULL, /* set_string */
    NULL, /* set_string_len */
    NULL, /* give_string_len */
    NULL, /* set_enum */
    NULL, /* set_fixed */
    NULL, /* give_fixed */
    NULL, /* get_size */
    NULL, /* get_by_index */
    NULL, /* get_by_name */
    NULL, /* get_discriminant */
    NULL, /* get_current_branch */
    NULL, /* append */
    NULL, /* add */
    NULL  /* set_branch */
};

static avro_value_iface_t  person_children_iface = {
    NULL, /* incref_iface */
    NULL, /* decref_iface */
    NULL, /* incref */
    NULL, /* decref */
    NULL, /* reset */
    person_children_get_type,
    person_children_get_schema,
    NULL, /* get_boolean */
    NULL, /* get_bytes */
    NULL, /* grab_bytes */
    NULL, /* get_double */
    NULL, /* get_float */
    NULL, /* get_int */
    NULL, /* get_long */
    NULL, /* get_null */
    NULL, /* get_string */
    NULL, /* grab_string */
    NULL, /* get_enum */
    NULL, /* get_fixed */
    NULL, /* grab_fixed */
    NULL, /* set_boolean */
    NULL, /* set_bytes */
    NULL, /* give_bytes */
    NULL, /* set_double */
    NULL, /* set_float */
    NULL, /* set_int */
    NULL, /* set_long */
    NULL, /* set_null */
    NULL, /* set_string */
    NULL, /* set_string_len */
    NULL, /* give_string_len */
    NULL, /* set_enum */
    NULL, /* set_fixed */
    NULL, /* give_fixed */
    person_children_get_size,
    person_children_get_by_index,
    NULL, /* get_by_name */
    NULL, /* get_discriminant */
    NULL, /* get_current_branch */
    NULL, /* append */
    NULL, /* add */
    NULL  /* set_branch */
};
