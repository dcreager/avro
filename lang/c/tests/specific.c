typedef struct avro_specific_list avro_specific_list_t;

typedef struct avro_specific_point avro_specific_point_t;

struct avro_specific_point {
       	int32_t x;
       	int32_t y;
};


typedef struct avro_specific_map_string avro_specific_map_string_t;

struct avro_specific_map_string {
       	avro_raw_map_t map;
};

typedef struct avro_specific_null_list avro_specific_null_list_t;

struct avro_specific_null_list {
       	int discriminant;
       	union {
	       	int null_val;
	       	avro_specific_list_t * list;
       	} branch;
};

struct avro_specific_list {
       	avro_specific_point_t point;
       	avro_specific_map_string_t attrs;
       	avro_specific_null_list_t next;
};

	
