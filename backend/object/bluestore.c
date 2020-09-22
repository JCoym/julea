/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
 * Copyright (C) 2020 Johannes Coym
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <julea.h>

#include <julea_bluestore.h>

struct JBackendData
{
	gchar* path;
};

typedef struct JBackendData JBackendData;

static gboolean
backend_create(gpointer backend_data, gchar const* namespace, gchar const* path, gpointer* backend_object)
{
	gchar* full_path;

	(void)backend_data;

	full_path = g_build_filename(namespace, path, NULL);

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);
    julea_bluestore_create(full_path);
	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	*backend_object = full_path;

	return TRUE;
}

static gboolean
backend_open(gpointer backend_data, gchar const* namespace, gchar const* path, gpointer* backend_object)
{
	gchar* full_path;

	(void)backend_data;

	full_path = g_build_filename(namespace, path, NULL);

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	*backend_object = full_path;

	return TRUE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer backend_object)
{
	gchar* full_path = backend_object;

	(void)backend_data;

	j_trace_file_begin(full_path, J_TRACE_FILE_DELETE);
    julea_bluestore_delete(full_path);
	j_trace_file_end(full_path, J_TRACE_FILE_DELETE, 0, 0);

	g_free(full_path);

	return TRUE;
}

static gboolean
backend_close(gpointer backend_data, gpointer backend_object)
{
	gchar* full_path = backend_object;

	(void)backend_data;

	j_trace_file_begin(full_path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(full_path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(full_path);

	return TRUE;
}

static gboolean
backend_status(gpointer backend_data, gpointer backend_object, gint64* modification_time, guint64* size)
{
    struct stat buf;
	gchar const* full_path = backend_object;
	(void)backend_data;

	j_trace_file_begin(full_path, J_TRACE_FILE_STATUS);
    julea_bluestore_status(full_path, &buf);
	j_trace_file_end(full_path, J_TRACE_FILE_STATUS, 0, 0);

	if (modification_time != NULL)
	{
		*modification_time = buf.st_mtime * G_USEC_PER_SEC;

#ifdef HAVE_STMTIM_TVNSEC
			*modification_time += buf.st_mtim.tv_nsec / 1000;
#endif
	}

	if (size != NULL)
	{
		*size = buf.st_size;
	}

	return TRUE;
}

static gboolean
backend_sync(gpointer backend_data, gpointer backend_object)
{
	gchar const* full_path = backend_object;

	(void)backend_data;

	j_trace_file_begin(full_path, J_TRACE_FILE_SYNC);
	j_trace_file_end(full_path, J_TRACE_FILE_SYNC, 0, 0);

	return TRUE;
}

static gboolean
backend_read(gpointer backend_data, gpointer backend_object, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	gchar const* full_path = backend_object;
    gsize br = 0;
	(void)backend_data;


	j_trace_file_begin(full_path, J_TRACE_FILE_READ);
    br = julea_bluestore_read(full_path, offset, (char**)&buffer, length);
	j_trace_file_end(full_path, J_TRACE_FILE_READ, length, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = br;
	}

	return (br == length);
}

static gboolean
backend_write(gpointer backend_data, gpointer backend_object, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	gchar const* full_path = backend_object;

    gsize bw = 0;

	(void)backend_data;

	j_trace_file_begin(full_path, J_TRACE_FILE_WRITE);
    bw = julea_bluestore_write(full_path, offset, (const char*)buffer, length);
	j_trace_file_end(full_path, J_TRACE_FILE_WRITE, length, offset);

	if (bytes_written != NULL)
	{
		*bytes_written = bw;
	}

	return (bw == length);
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
    //TODO: use julea_bluestore_mount when bluestore exists

	JBackendData* bd;

	bd = g_slice_new(JBackendData);
	bd->path = g_strdup(path);

	julea_bluestore_init(path);

	*backend_data = bd;

	return TRUE;
}

static void
backend_fini(gpointer backend_data)
{
	JBackendData* bd = backend_data;

    julea_bluestore_umount();

	g_free(bd->path);
	g_slice_free(JBackendData, bd);
}

static JBackend bluestore_backend = {
	.type = J_BACKEND_TYPE_OBJECT,
	.component = J_BACKEND_COMPONENT_CLIENT | J_BACKEND_COMPONENT_SERVER,
	.object = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_create = backend_create,
		.backend_delete = backend_delete,
		.backend_open = backend_open,
		.backend_close = backend_close,
		.backend_status = backend_status,
		.backend_sync = backend_sync,
		.backend_read = backend_read,
		.backend_write = backend_write }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &bluestore_backend;
}
