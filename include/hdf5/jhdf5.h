#ifndef h5_julea_plugin_h
#define h5_julea_plugin_h

#include <julea-config.h>
#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <bson.h>
#include <unistd.h>
#include <sys/types.h>

struct SQF_t;

typedef struct SQF_t
{
	char *name;
	JKV *kv;
	bson_oid_t id;
} SQF_t; /* structure for file*/

typedef struct SQG_t
{
	char *location;
	char *name;
	JKV *kv;
	bson_oid_t id;
} SQG_t; /* structure for group*/

typedef struct SQD_t
{
	char *name;
	char *location;
	int position;
	size_t data_size;
	JKV *size;
	bson_oid_t size_id;
	JDistribution *distribution;
	JDistributedObject *object;
	JKV *_distribution;
	bson_oid_t dist_id;
	bson_oid_t id;
	JKV *space;
	size_t space_size;
	bson_oid_t space_id;
	JKV *type;
	bson_oid_t type_id;
	size_t type_size;
} SQD_t; /* structure for dataset*/

typedef struct SQA_t
{
	char *location;
	char *name;
	size_t data_size;
	JKV *kv;
	bson_oid_t id;
	JKV *space;
	bson_oid_t space_id;
	size_t space_size;
	JKV *type;
	bson_oid_t type_id;
	size_t type_size;
} SQA_t; /*structure for attribute*/


typedef struct h5julea_fapl_t {
  //JKV *kv;
  //__off64_t offset;
  char *name;
} h5julea_fapl_t;

char* j_hdf5_encode_type (const char*, hid_t*, hid_t, size_t*);

char* j_hdf5_encode_space (const char*, hid_t*, hid_t, size_t*);

bson_t* j_hdf5_serialize (bson_oid_t*, char*, const void*, size_t);

bson_t* j_hdf5_serialize_size (bson_oid_t*, char*, size_t);

bson_t* j_hdf5_serialize_distribution (bson_oid_t*, JDistribution*);

void j_hdf5_deserialize (const bson_t*, void*, size_t);

void j_hdf5_deserialize_distribution (const bson_t*, SQD_t*);

void j_hdf5_deserialize_meta (const bson_t*, const bson_oid_t*, size_t*);

char* create_path(const char*, char*);

H5PL_type_t H5PLget_plugin_type(void);

const void *H5PLget_plugin_info(void);

#endif
