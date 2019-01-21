#include <errno.h>
#include <assert.h>
#include <fcntl.h>
#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <hdf5/jhdf5.h>

#include <julea-config.h>
#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea-internal.h>
#include <glib.h>
#include <bson.h>

#define _GNU_SOURCE

#define LOG 520

#define ERRORMSG(...)    \
	printf(__VA_ARGS__); \
	printf("\n");        \
	assert(false);       \
	exit(1);

#define COUNT_MAX 2147479552

static herr_t H5VL_extlog_fapl_free(void *info);
static void *H5VL_extlog_fapl_copy(const void *info);

static herr_t H5VL_log_init(hid_t vipl_id);
static herr_t H5VL_log_term(hid_t vtpl_id);

/* Atrribute callbacks */
static void *H5VL_extlog_attr_create(void *obj, H5VL_loc_params_t loc_params, const char *attr_name, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
static void *H5VL_extlog_attr_open(void *obj, H5VL_loc_params_t loc_params, const char *attr_name, hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_extlog_attr_read(void *attr, hid_t dtype_id, void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_extlog_attr_write(void *attr, hid_t dtype_id, const void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_extlog_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_extlog_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_extlog_dataset_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_extlog_dataset_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_extlog_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_extlog_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_extlog_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_extlog_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* File callbacks */
static void *H5VL_extlog_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_extlog_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_extlog_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *H5VL_extlog_group_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *H5VL_extlog_group_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_extlog_group_close(void *grp, hid_t dxpl_id, void **req);


hid_t native_plugin_id = -1;

static const H5VL_class_t H5VL_log_g = {
	0,
	LOG,
	"extlog",	  /* name */
	H5VL_log_init, /* initialize */
	H5VL_log_term, /* terminate */
	sizeof(h5julea_fapl_t),
	H5VL_extlog_fapl_copy,
	H5VL_extlog_fapl_free,
	{
		/* attribute_cls */
		H5VL_extlog_attr_create, /* create */
		H5VL_extlog_attr_open,   /* open */
		H5VL_extlog_attr_read,   /* read */
		H5VL_extlog_attr_write,  /* write */
		H5VL_extlog_attr_get,	/* get */
		NULL,					 //H5VL_extlog_attr_specific,              /* specific */
		NULL,					 //H5VL_extlog_attr_optional,              /* optional */
		H5VL_extlog_attr_close   /* close */
	},
	{
		/* dataset_cls */
		H5VL_extlog_dataset_create, /* create */
		H5VL_extlog_dataset_open,   /* open */
		H5VL_extlog_dataset_read,   /* read */
		H5VL_extlog_dataset_write,  /* write */
		H5VL_extlog_dataset_get,	/* get */
		NULL,						//H5VL_extlog_dataset_specific,          /* specific */
		NULL,						//H5VL_extlog_dataset_optional,          /* optional */
		H5VL_extlog_dataset_close   /* close */
	},
	{
		/* datatype_cls */
		NULL, //H5VL_extlog_datatype_commit, /* commit */
		NULL, //H5VL_extlog_datatype_open,   /* open */
		NULL, //H5VL_extlog_datatype_get,	/* get_size */
		NULL,						 //H5VL_extlog_datatype_specific,         /* specific */
		NULL,						 //H5VL_extlog_datatype_optional,         /* optional */
		NULL, //H5VL_extlog_datatype_close   /* close */
	},
	{
		/* file_cls */
		H5VL_extlog_file_create, /* create */
		H5VL_extlog_file_open,   /* open */
		NULL, //H5VL_extlog_file_get,	/* get */
		NULL,					 //H5VL_extlog_file_specific,            /* specific */
		NULL,					 //H5VL_extlog_file_optional,            /* optional */
		H5VL_extlog_file_close   /* close */
	},
	{
		/* group_cls */
		H5VL_extlog_group_create, /* create */
		H5VL_extlog_group_open,   /* open */
		NULL, //H5VL_extlog_group_get,	/* get */
		NULL,					  //H5VL_extlog_group_specific,           /* specific */
		NULL,					  //H5VL_extlog_group_optional,           /* optional */
		H5VL_extlog_group_close   /* close */
	},
	{
		/* link_cls */
		NULL, //H5VL_extlog_link_create,                /* create */
		NULL, //H5VL_extlog_link_copy,                  /* copy */
		NULL, //H5VL_extlog_link_move,                  /* move */
		NULL, //H5VL_extlog_link_get,                   /* get */
		NULL, //H5VL_extlog_link_specific,              /* specific */
		NULL, //H5VL_extlog_link_optional,              /* optional */
	},
	{
		/* object_cls */
		NULL, //H5VL_extlog_object_open,                        /* open */
		NULL, //H5VL_extlog_object_copy,                /* copy */
		NULL, //H5VL_extlog_object_get,                 /* get */
		NULL, //H5VL_extlog_object_specific,                    /* specific */
		NULL, //H5VL_extlog_object_optional,            /* optional */
	},
	{NULL,
	 NULL,
	 NULL},
	NULL};

char *err_msg = 0; /* pointer to an error string */

h5julea_fapl_t *ginfo = NULL;

static void *H5VL_extlog_fapl_copy(const void *info)
{
	const h5julea_fapl_t* fapl_source = (const h5julea_fapl_t*) info;
	h5julea_fapl_t* fapl_target = (h5julea_fapl_t*) malloc(sizeof(*fapl_target));
	memcpy(fapl_target, fapl_source, sizeof(*fapl_source));
	fapl_target->name  = fapl_source->name;
	return (void *)fapl_target;
}

static herr_t H5VL_extlog_fapl_free(void *info __attribute__((unused)))
{
	herr_t err = 0;
	return err;
}

static herr_t H5VL_log_init(hid_t vipl_id __attribute__((unused)))
{
	native_plugin_id = H5VLget_plugin_id("native");
	assert(native_plugin_id > 0);
	//printf("------- LOG INIT\n");
	return 0;
}

static herr_t H5VL_log_term(hid_t vtpl_id __attribute__((unused)))
{
	return 0;
}

char*
j_hdf5_encode_type (const char *property, hid_t *type_id, hid_t cpl_id, size_t *type_size)
{
	char* type_buf;
	H5Pget(cpl_id, property, type_id);
	assert(-1 != *type_id);
	H5Tencode(*type_id, NULL, type_size);
	type_buf = (char*) malloc(*type_size);
	H5Tencode(*type_id, type_buf, type_size);
	return type_buf;
}

char*
j_hdf5_encode_space (const char *property, hid_t *space_id, hid_t cpl_id, size_t *space_size)
{
	char* space_buf;
	H5Pget(cpl_id, property, space_id);
	assert(-1 != *space_id);
	H5Sencode(*space_id, NULL, space_size);
	space_buf = (char*) malloc(*space_size);
	H5Sencode(*space_id, space_buf, space_size);
	return space_buf;
}

bson_t*
j_hdf5_serialize (char *name, const void *data, size_t data_size)
{
	bson_t* b;

	j_trace_enter(G_STRFUNC, NULL);

	b = bson_new();
	bson_init(b);

	bson_append_utf8(b, "name", -1, name, -1);
	bson_append_binary(b, "data", -1, BSON_SUBTYPE_BINARY, data, data_size);
	bson_append_int32(b, "size", -1, (int32_t)data_size);

	j_trace_leave(G_STRFUNC);

	return b;
}

bson_t*
j_hdf5_serialize_size (char *name, size_t data_size)
{
	bson_t* b;

	j_trace_enter(G_STRFUNC, NULL);

	b = bson_new();
	bson_init(b);

	bson_append_utf8(b, "name", -1, name, -1);
	bson_append_int32(b, "size", -1, (int32_t)data_size);

	j_trace_leave(G_STRFUNC);

	return b;
}

bson_t*
j_hdf5_serialize_distribution (JDistribution* distribution)
{
	bson_t* b;
	bson_t* b_distribution;

	j_trace_enter(G_STRFUNC, NULL);

	b_distribution = j_distribution_serialize(distribution);

	b = bson_new();
	bson_init(b);

	bson_append_document(b, "distribution", -1, b_distribution);

	bson_destroy(b_distribution);

	j_trace_leave(G_STRFUNC);

	return b;
}

void
j_hdf5_deserialize (const bson_t* b, void *data, size_t data_size)
{
	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;

	g_return_if_fail(data_size != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "data") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&data_size, (const uint8_t **) &buf);
		}
	}
	memcpy(data, buf, data_size);
	j_trace_leave(G_STRFUNC);
}

void
j_hdf5_deserialize_meta (const bson_t* b, size_t* data_size)
{
	bson_iter_t iterator;

	g_return_if_fail(data_size != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "size") == 0)
		{
			*data_size = bson_iter_int32(&iterator);
		}
	}
	j_trace_leave(G_STRFUNC);
}

void
j_hdf5_deserialize_distribution (const bson_t* b, JHD_t* d)
{
	bson_iter_t iterator;

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		else if (g_strcmp0(key, "distribution") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_distribution[1];
			
			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_distribution, data, len);
			d->distribution = j_distribution_new_from_bson(b_distribution);
			bson_destroy(b_distribution);
		}
	}

	j_trace_leave(G_STRFUNC);
}

char* create_path(const char* name, char* prev_location) {
	static const char* sep = "/";
	char* res = NULL;

	if (NULL != prev_location && NULL != name) {
		res = (char*) malloc(strlen(prev_location) + strlen(sep) + strlen(name) + 1);
		strcpy(res, prev_location);
		strcat(res, sep);
		strcat(res, name);
	} 
	else {
		ERRORMSG("Couldn't create path");
	}
	return res;
}

/*-------------------------------------------------------------------------
* Function:	H5VL_extlog_attr_create
*
* Purpose:	Creates an attribute on an object.
*-------------------------------------------------------------------------
*/

static void *
H5VL_extlog_attr_create(void *obj, H5VL_loc_params_t loc_params, const char *attr_name, hid_t acpl_id, hid_t aapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *attribute;
	hid_t space_id;
	size_t space_size;
	char* space_buf;
	int ndims;
	hsize_t *maxdims;
	hsize_t *dims;
	hid_t type_id;
	size_t type_size;
	char* type_buf;
	size_t data_size;
	bson_t* value;
	JBatch* batch;
	JBatch* batch2;
	char* typeloc;
	char* spaceloc;

	attribute = (JHA_t *)malloc(sizeof(*attribute));

	type_buf = j_hdf5_encode_type("attr_type_id", &type_id, acpl_id, &type_size);
	space_buf = j_hdf5_encode_space("attr_space_id", &space_id, acpl_id, &space_size);
	
	ndims = H5Sget_simple_extent_ndims(space_id);
	maxdims = malloc(sizeof(hsize_t) * ndims);
	dims = malloc(sizeof(hsize_t) * ndims);
	H5Sget_simple_extent_dims(/* in */ space_id, /* out */ dims, /* out */ maxdims);
	data_size = H5Tget_size(type_id);

	for (int i = 0; i < ndims; ++i)
	{
		data_size *= dims[i];
	}

	switch (loc_params.obj_type)
	{
	case H5I_FILE:
		break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		attribute->location = create_path(attr_name, o->location);

		j_trace_enter(G_STRFUNC, NULL);

		attribute->kv = j_kv_new("hdf5", attribute->location);

		j_trace_leave(G_STRFUNC);

		attribute->name = g_strdup(attr_name);
		attribute->data_size = data_size;
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:

	{
		JHD_t *o = (JHD_t *)obj;
		attribute->location = create_path(attr_name, o->location);

		j_trace_enter(G_STRFUNC, NULL);

		attribute->kv = j_kv_new("hdf5", attribute->location);

		j_trace_leave(G_STRFUNC);

		attribute->data_size = data_size;
		attribute->name = g_strdup(attr_name);
	}
	break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_REFERENCE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	j_trace_enter(G_STRFUNC, NULL);
	typeloc = g_strdup(attribute->location);
	attribute->type = j_kv_new("hdf5", strcat(typeloc, "_type"));
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize(typeloc, type_buf, type_size);
	j_kv_put(attribute->type, value, batch);
	j_batch_execute(batch);
	attribute->type_size = type_size;
	j_trace_leave(G_STRFUNC);

	j_trace_enter(G_STRFUNC, NULL);
	spaceloc = g_strdup(attribute->location);
	attribute->space = j_kv_new("hdf5", strcat(spaceloc, "_space"));
	batch2 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize(spaceloc, space_buf, space_size);
	j_kv_put(attribute->space, value, batch2);
	j_batch_execute(batch2);
	attribute->space_size = space_size;
	j_trace_leave(G_STRFUNC);

	return (void *)attribute;
}

/*-------------------------------------------------------------------------
* Function:	H5VL_extlog_attr_open
*
* Purpose:	Opens a attr inside a native h5 file
*-------------------------------------------------------------------------
*/

static void *
H5VL_extlog_attr_open(void *obj, H5VL_loc_params_t loc_params, const char *attr_name,
					  hid_t aapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *attribute;
	JKV *kv;
	bson_t data[1];
	JBatch *batch;
	JKV *space;
	bson_t spacedata[1];
	JBatch *batch2;
	JKV *type;
	bson_t typedata[1];
	JBatch *batch3;
	char* typeloc;
	char* spaceloc;

	attribute = (JHA_t *)malloc(sizeof(*attribute));
	attribute->name = g_strdup(attr_name);

	switch (loc_params.obj_type)
	{
	case H5I_FILE:
		break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		attribute->location = create_path(attr_name, o->location);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
	{
		JHD_t *o = (JHD_t *)obj;
		attribute->location = create_path(attr_name, o->location);
	}
	break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_REFERENCE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	typeloc = (char*) malloc(strlen(attribute->location) + 6);
	spaceloc = (char*) malloc(strlen(attribute->location) + 7);

	j_trace_enter(G_STRFUNC, NULL);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	kv = j_kv_new("hdf5", attribute->location);
	j_kv_get(kv, data, batch);
	if (j_batch_execute(batch))
	{
		j_hdf5_deserialize_meta(data, &(attribute->data_size));
		attribute->kv = kv;
	}
	j_trace_leave(G_STRFUNC);

	j_trace_enter(G_STRFUNC, NULL);
	batch2 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	strcpy(spaceloc, attribute->location);
	strcat(spaceloc, "_space");
	space = j_kv_new("hdf5", spaceloc);
	j_kv_get(space, spacedata, batch2);
	if (j_batch_execute(batch2))
	{
		j_hdf5_deserialize_meta(spacedata, &(attribute->space_size));
		attribute->space = space;
	}
	j_trace_leave(G_STRFUNC);

	batch3 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	strcpy(typeloc, attribute->location);
	strcat(typeloc, "_type");
	type = j_kv_new("hdf5", typeloc);
	j_kv_get(type, typedata, batch3);
	if (j_batch_execute(batch3))
	{
		j_hdf5_deserialize_meta(typedata, &(attribute->type_size));
		attribute->type = type;
	}

	return (void *)attribute;
}

/*-------------------------------------------------------------------------
	* Function:	H5VL_extlog_attr_read
	*
	* Purpose:	Reads in data from attribute
	*-------------------------------------------------------------------------
	*/
static herr_t
H5VL_extlog_attr_read(void *attr, hid_t dtype_id __attribute__((unused)), void *buf, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *d;
	bson_t b[1];
	JBatch* batch;
	d = (JHA_t *)attr;
	
	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_kv_get(d->kv, b, batch);
	j_batch_execute(batch);
	j_hdf5_deserialize(b, buf, d->data_size);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/*-------------------------------------------------------------------------
	* Function:	H5VL_extlog_attr_write
	*
	* Purpose:	Writes out data to attribute
	*-------------------------------------------------------------------------
	*/
static herr_t
H5VL_extlog_attr_write(void *attr, hid_t dtype_id __attribute__((unused)), const void *buf, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *d = (JHA_t *)attr;
	bson_t* value;
	JBatch* batch;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize(&(d->id), d->location, buf, d->data_size);
	j_kv_put(d->kv, value, batch);
	j_batch_execute(batch);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/*-------------------------------------------------------------------------
* Function:	H5VL_extlog_attr_get
*
* Purpose:	Gets certain information about an attribute
*-------------------------------------------------------------------------
*/
static herr_t
H5VL_extlog_attr_get(void *attr, H5VL_attr_get_t get_type, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)), va_list arguments)
{
	herr_t ret_value = 0;
	JHA_t *d;
	bson_t b[1];
	JBatch* batch;
	char* type_buf;
	char* space_buf;

	d = (JHA_t *)attr;

	switch (get_type)
	{
	case H5VL_ATTR_GET_ACPL:
		break;
	case H5VL_ATTR_GET_INFO:
		break;
	case H5VL_ATTR_GET_NAME:
		break;
	case H5VL_ATTR_GET_SPACE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);
		g_assert(d != NULL);
		g_assert(d->space != NULL);
		space_buf = malloc(d->space_size);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->space, b, batch);
		if (j_batch_execute(batch))
		{
			j_hdf5_deserialize(b, space_buf, d->space_size);
		}
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Sdecode(space_buf);
	}
	break;
	case H5VL_ATTR_GET_STORAGE_SIZE:
		break;
	case H5VL_ATTR_GET_TYPE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);
		g_assert(d != NULL);
		g_assert(d->type != NULL);
		type_buf = malloc(d->type_size);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->type, b, batch);
		if (j_batch_execute(batch))
		{
			j_hdf5_deserialize(b, type_buf, d->type_size);
		}
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Tdecode(type_buf);
	}
	break;
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}

	return ret_value;
}

/*-------------------------------------------------------------------------
* Function:	H5VL_extlog_attr_close
*
* Purpose:	Closes an attribute
*-------------------------------------------------------------------------
*/
static herr_t
H5VL_extlog_attr_close(void *attr, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *a = (JHA_t *)attr;

	if (a->kv != NULL)
	{
		j_kv_unref(a->kv);
	}

	if (a->space != NULL)
	{
		j_kv_unref(a->space);
	}

	if (a->type != NULL)
	{
		j_kv_unref(a->type);
	}

	g_free(a->name);
	a->name = NULL;
	free(a->location);
	a->location = NULL;
	free(a);
	return 1;
}

static void *
H5VL_extlog_file_create(const char *fname, unsigned flags __attribute__((unused)), hid_t fcpl_id __attribute__((unused)), hid_t fapl_id, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHF_t *file;

	ginfo = (h5julea_fapl_t *)H5VL_extlog_fapl_copy(H5Pget_vol_info(fapl_id));
	file = (JHF_t *)malloc(sizeof(*file));
	file->name = g_strdup(fname);
	return (void *)file;
}

static void *
H5VL_extlog_file_open(const char *fname, unsigned flags __attribute__((unused)), hid_t fapl_id, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHF_t *file;
	
	ginfo = (h5julea_fapl_t *)H5VL_extlog_fapl_copy(H5Pget_vol_info(fapl_id));
	file = (JHF_t *)malloc(sizeof(*file));
	file->name = g_strdup(fname);
	return (void *)file;
}

static herr_t
H5VL_extlog_file_close(void *file, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHF_t *f = (JHF_t *)file;
	g_free(f->name);
	f->name = NULL;
	free(f);
	H5VL_extlog_fapl_free(ginfo);
	return 1;
}

static void *
H5VL_extlog_group_create(void *obj, H5VL_loc_params_t loc_params, const char *name,
						 hid_t gcpl_id __attribute__((unused)), hid_t gapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHG_t *group;
	group = (JHG_t *)malloc(sizeof(*group));

	switch (loc_params.obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		group->location = create_path(name, o->name);
		group->name = g_strdup(name);
	}
	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		group->location = create_path(name, o->location);
		group->name = g_strdup(name);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_REFERENCE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */
	return (void *)group;
}

static void *H5VL_extlog_group_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t gapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHG_t *group;
	group = (JHG_t *)malloc(sizeof(*group));

	switch (loc_params.obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		group->location = create_path(name, o->name);
	}
	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		group->location = create_path(name, o->location);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_REFERENCE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	group->name = g_strdup(name);
	return (void *)group;
}

static herr_t
H5VL_extlog_group_close(void *grp, hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHG_t *g = (JHG_t *)grp;
	g_free(g->name);
	free(g->location);
	free(g);
	return 1;
}

static void *
H5VL_extlog_dataset_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id  __attribute__((unused)), hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHD_t *dset;
	hid_t space_id;
	size_t space_size;
	char* space_buf;
	int ndims;
	hsize_t *maxdims;
	hsize_t *dims;
	hid_t type_id;
	size_t type_size;
	char* type_buf;
	size_t data_size;
	bson_t* value;
	JBatch* batch;
	char* typeloc;
	char* spaceloc;
	char* distloc;
	char* sizeloc;

	dset = (JHD_t *)malloc(sizeof(*dset));

	type_buf = j_hdf5_encode_type("dataset_type_id", &type_id, dcpl_id, &type_size);
	space_buf = j_hdf5_encode_space("dataset_space_id", &space_id, dcpl_id, &space_size);

	ndims = H5Sget_simple_extent_ndims(space_id);
	maxdims = malloc(sizeof(hsize_t) * ndims);
	dims = malloc(sizeof(hsize_t) * ndims);
	H5Sget_simple_extent_dims(/* in */ space_id, /* out */ dims, /* out */ maxdims);
	data_size = H5Tget_size(type_id);
	for (int i = 0; i < ndims; ++i)
	{
		data_size *= dims[i];
	}
	dset->data_size = data_size;

	switch (loc_params.obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		dset->location = create_path(name, o->name);
		
		j_trace_enter(G_STRFUNC, NULL);

		dset->distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
		dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);

		j_trace_leave(G_STRFUNC);
	}

	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		dset->location = create_path(name, o->location);

		j_trace_enter(G_STRFUNC, NULL);

		dset->distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
		dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);

		j_trace_leave(G_STRFUNC);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_REFERENCE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	distloc = (char*) malloc(strlen(dset->location) + 6);
	typeloc = (char*) malloc(strlen(dset->location) + 6);
	spaceloc = (char*) malloc(strlen(dset->location) + 7);
	sizeloc = (char*) malloc(strlen(dset->location) + 6);

	j_trace_enter(G_STRFUNC, NULL);
	strcpy(distloc, dset->location);
	strcat(distloc, "_dist");
	dset->_distribution = j_kv_new("hdf5", distloc);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize_distribution(&(dset->dist_id), dset->distribution);
	j_kv_put(dset->_distribution, value, batch);
	j_batch_execute(batch);
	j_trace_leave(G_STRFUNC);

	j_trace_enter(G_STRFUNC, NULL);
	strcpy(typeloc, dset->location);
	strcat(typeloc, "_type");
	dset->type = j_kv_new("hdf5", typeloc);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize(typeloc, type_buf, type_size);
	j_kv_put(dset->type, value, batch);
	j_batch_execute(batch);
	dset->type_size = type_size;
	j_trace_leave(G_STRFUNC);

	j_trace_enter(G_STRFUNC, NULL);
	strcpy(spaceloc, dset->location);
	strcat(spaceloc, "_space");
	dset->space = j_kv_new("hdf5", spaceloc);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize(spaceloc, space_buf, space_size);
	j_kv_put(dset->space, value, batch);
	j_batch_execute(batch);
	dset->space_size = space_size;
	j_trace_leave(G_STRFUNC);

	j_trace_enter(G_STRFUNC, NULL);
	strcpy(sizeloc, dset->location);
	strcat(sizeloc, "_size");
	dset->size = j_kv_new("hdf5", sizeloc);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize_size(sizeloc, data_size);
	j_kv_put(dset->size, value, batch);
	j_batch_execute(batch);
	j_trace_leave(G_STRFUNC);

	dset->name = g_strdup(name);
	return (void *)dset;
}

static void *
H5VL_extlog_dataset_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dapl_id  __attribute__((unused)), hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHD_t *dset;
	JKV *space;
	bson_t spacedata[1];
	JBatch *batch2;
	JKV *type;
	bson_t typedata[1];
	JBatch *batch3;
	JKV *dist;
	bson_t distdata[1];
	JBatch *batch4;
	JKV *size;
	bson_t sizedata[1];
	JBatch *batch5;
	char* typeloc;
	char* spaceloc;
	char* distloc;
	char* sizeloc;

	dset = (JHD_t *)malloc(sizeof(*dset));
	dset->name = g_strdup(name);

	switch (loc_params.obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		dset->location = create_path(name, o->name);
	}

	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		dset->location = create_path(name, o->location);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_REFERENCE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	distloc = (char*) malloc(strlen(dset->location) + 6);
	typeloc = (char*) malloc(strlen(dset->location) + 6);
	spaceloc = (char*) malloc(strlen(dset->location) + 7);
	sizeloc = (char*) malloc(strlen(dset->location) + 6);

	batch2 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	strcpy(spaceloc, dset->location);
	strcat(spaceloc, "_space");
	space = j_kv_new("hdf5", spaceloc);
	j_kv_get(space, spacedata, batch2);
	if (j_batch_execute(batch2))
	{
		j_hdf5_deserialize_meta(spacedata, &(dset->space_size));
		dset->space = space;
	}

	batch3 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	strcpy(typeloc, dset->location);
	strcat(typeloc, "_type");
	type = j_kv_new("hdf5", typeloc);
	j_kv_get(type, typedata, batch3);
	if (j_batch_execute(batch3))
	{
		j_hdf5_deserialize_meta(typedata, &(dset->type_size));
		dset->type = type;
	}

	batch4 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	strcpy(distloc, dset->location);
	strcat(distloc, "_dist");
	dist = j_kv_new("hdf5", distloc);
	j_kv_get(dist, distdata, batch4);
	if (j_batch_execute(batch4))
	{
		j_hdf5_deserialize_distribution(distdata, dset);
	}
	dset->_distribution = dist;

	dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);

	batch5 = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	strcpy(sizeloc, dset->location);
	strcat(sizeloc, "_size");
	size = j_kv_new("hdf5", sizeloc);
	j_kv_get(size, sizedata, batch5);
	if (j_batch_execute(batch5))
	{
		j_hdf5_deserialize_meta(sizedata, &(dset->size_id), &(dset->data_size));
		dset->size = size;
	}

	return (void *)dset;
}

static herr_t
H5VL_extlog_dataset_read(void *dset, hid_t mem_type_id  __attribute__((unused)), hid_t mem_space_id  __attribute__((unused)),
						 hid_t file_space_id  __attribute__((unused)), hid_t plist_id  __attribute__((unused)), void *buf, void **req  __attribute__((unused)))
{
	JBatch* batch;
	JHD_t *d;
	guint64 bytes_read;

	d = (JHD_t *)dset;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	bytes_read = 0;

	g_assert(buf != NULL);

	g_assert(d->object != NULL);

	j_distributed_object_read(d->object, buf, d->data_size, 0, &bytes_read, batch);

	j_batch_execute(batch);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/*-------------------------------------------------------------------------
* Function:	H5VL_extlog_dataset_get
*
* Purpose:	Gets certain information about a data set
*-------------------------------------------------------------------------
*/
static herr_t
H5VL_extlog_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)), va_list arguments)
{
	herr_t ret_value = 0;
	JHD_t *d;
	bson_t b[1];
	JBatch* batch;
	char* type_buf;
	char* space_buf;

	d = (JHD_t *)dset;

	switch (get_type)
	{
	case H5VL_DATASET_GET_DAPL:
		break;
	case H5VL_DATASET_GET_DCPL:
		break;
	case H5VL_DATASET_GET_OFFSET:
		break;
	case H5VL_DATASET_GET_SPACE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);
		space_buf = malloc(d->space_size);
		
		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->space, b, batch);
		j_batch_execute(batch);
		j_hdf5_deserialize(b, space_buf, d->space_size);
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Sdecode(space_buf);
	}
		break;
	case H5VL_DATASET_GET_SPACE_STATUS:
		break;
	case H5VL_DATASET_GET_STORAGE_SIZE:
		break;
	case H5VL_DATASET_GET_TYPE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);
		type_buf = malloc(d->type_size);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->type, b, batch);
		j_batch_execute(batch);
		j_hdf5_deserialize(b, type_buf, d->type_size);
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Tdecode(type_buf);
	}
		break;
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}

	return ret_value;
}

static herr_t
H5VL_extlog_dataset_write(void *dset, hid_t mem_type_id  __attribute__((unused)), hid_t mem_space_id  __attribute__((unused)),
						  hid_t file_space_id  __attribute__((unused)), hid_t plist_id  __attribute__((unused)), const void *buf, void **req  __attribute__((unused)))
{
	JBatch* batch;
	JHD_t *d;
	guint64 bytes_written;

	d = (JHD_t *)dset;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	bytes_written = 0;

	j_distributed_object_write(d->object, buf, d->data_size, 0, &bytes_written, batch);

	j_batch_execute(batch);

	j_trace_leave(G_STRFUNC);

	return 1;
}

static herr_t
H5VL_extlog_dataset_close(void *dset, hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHD_t *d = (JHD_t *)dset;
	if (d->distribution != NULL)
	{
		j_distribution_unref(d->distribution);
	}
	if (d->_distribution != NULL)
	{
		j_kv_unref(d->_distribution);
	}

	if (d->space != NULL)
	{
		j_kv_unref(d->space);
	}

	if (d->type != NULL)
	{
		j_kv_unref(d->type);
	}

	if (d->object != NULL)
	{
		j_distributed_object_unref(d->object);
	}

	g_free(d->name);
	free(d->location);
	free(d);
	return 1;
}

/* return the library type which should always be H5PL_TYPE_VOL */
H5PL_type_t H5PLget_plugin_type(void)
{
	return H5PL_TYPE_VOL;
}
/* return a pointer to the plugin structure defining the VOL plugin with all callbacks */
const void *H5PLget_plugin_info(void)
{
	return &H5VL_log_g;
}
