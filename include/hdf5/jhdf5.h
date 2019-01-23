#ifndef h5_julea_plugin_h
#define h5_julea_plugin_h

#include <julea-config.h>
#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <bson.h>
#include <unistd.h>
#include <sys/types.h>

struct JHF_t;

typedef struct JHF_t
{
	char *name;
} JHF_t; /* structure for file*/

typedef struct JHG_t
{
	char *location;
	char *name;
} JHG_t; /* structure for group*/

typedef struct JHD_t
{
	char *location;
	char *name;
	size_t data_size;
	JDistribution *distribution;
	JDistributedObject *object;
	JKV *kv;
} JHD_t; /* structure for dataset*/

typedef struct JHA_t
{
	char *location;
	char *name;
	size_t data_size;
	JKV *kv;
	JKV *ts;
} JHA_t; /*structure for attribute*/

typedef struct h5julea_fapl_t
{
	char *name;
} h5julea_fapl_t;

char *j_hdf5_encode_type(const char *, hid_t *, hid_t, size_t *);

char *j_hdf5_encode_space(const char *, hid_t *, hid_t, size_t *);

bson_t *j_hdf5_serialize(const void *, size_t);

bson_t *j_hdf5_serialize_ts (const void *, size_t, const void *, size_t);

bson_t *j_hdf5_serialize_dataset(const void *, size_t, const void *, size_t, size_t, JDistribution *);

void j_hdf5_deserialize(const bson_t *, void *, size_t);

void *j_hdf5_deserialize_type (const bson_t*);

void *j_hdf5_deserialize_space (const bson_t*);

void j_hdf5_deserialize_dataset(const bson_t *, JHD_t *, size_t *);

void j_hdf5_deserialize_meta(const bson_t *, size_t *);

char *create_path(const char *, char *);

H5PL_type_t H5PLget_plugin_type(void);

const void *H5PLget_plugin_info(void);

#endif
