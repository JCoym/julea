/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <jbackend.h>
#include <jbackend-internal.h>

#include <jtrace-internal.h>

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

static
GModule*
j_backend_load (gchar const* name, gchar const* component, JBackendType type, JBackend** backend)
{
	JBackend* (*module_backend_info) (JBackendType) = NULL;

	JBackend* tmp_backend = NULL;
	GModule* module = NULL;
	gchar* cpath = NULL;
	gchar* path = NULL;

#ifdef JULEA_BACKEND_PATH_BUILD
	cpath = g_build_filename(JULEA_BACKEND_PATH_BUILD, component, NULL);
	path = g_module_build_path(cpath, name);
	module = g_module_open(path, G_MODULE_BIND_LOCAL);
	g_free(cpath);
	g_free(path);
#endif

	if (module == NULL)
	{
		cpath = g_build_filename(JULEA_BACKEND_PATH, component, NULL);
		path = g_module_build_path(cpath, name);
		module = g_module_open(path, G_MODULE_BIND_LOCAL);
		g_free(cpath);
		g_free(path);
	}

	if (module != NULL)
	{
		g_module_symbol(module, "backend_info", (gpointer*)&module_backend_info);

		g_assert(module_backend_info != NULL);

		j_trace_enter("backend_info", "%d", type);
		tmp_backend = module_backend_info(type);
		j_trace_leave("backend_info");

		if (tmp_backend != NULL)
		{
			g_assert(tmp_backend->type == type);

			if (type == J_BACKEND_TYPE_DATA)
			{
				g_assert(tmp_backend->u.data.init != NULL);
				g_assert(tmp_backend->u.data.fini != NULL);
				g_assert(tmp_backend->u.data.create != NULL);
				g_assert(tmp_backend->u.data.delete != NULL);
				g_assert(tmp_backend->u.data.open != NULL);
				g_assert(tmp_backend->u.data.close != NULL);
				g_assert(tmp_backend->u.data.status != NULL);
				g_assert(tmp_backend->u.data.sync != NULL);
				g_assert(tmp_backend->u.data.read != NULL);
				g_assert(tmp_backend->u.data.write != NULL);
			}
			else if (type == J_BACKEND_TYPE_META)
			{
				g_assert(tmp_backend->u.meta.init != NULL);
				g_assert(tmp_backend->u.meta.fini != NULL);
				g_assert(tmp_backend->u.meta.batch_start != NULL);
				g_assert(tmp_backend->u.meta.batch_execute != NULL);
				g_assert(tmp_backend->u.meta.put != NULL);
				g_assert(tmp_backend->u.meta.delete != NULL);
				g_assert(tmp_backend->u.meta.get != NULL);
				g_assert(tmp_backend->u.meta.get_all != NULL);
				g_assert(tmp_backend->u.meta.get_by_value != NULL);
				g_assert(tmp_backend->u.meta.iterate != NULL);
			}
		}
	}

	*backend = tmp_backend;

	return module;
}

GModule*
j_backend_load_client (gchar const* name, JBackendType type, JBackend** backend)
{
	return j_backend_load(name, "client", type, backend);
}

GModule*
j_backend_load_server (gchar const* name, JBackendType type, JBackend** backend)
{
	return j_backend_load(name, "server", type, backend);
}

gboolean
j_backend_data_init (JBackend* backend, gchar const* path)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	j_trace_enter("backend_init", "%s", path);
	ret = backend->u.data.init(path);
	j_trace_leave("backend_init");

	return ret;
}

void
j_backend_data_fini (JBackend* backend)
{
	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_DATA);

	j_trace_enter("backend_fini", NULL);
	backend->u.data.fini();
	j_trace_leave("backend_fini");
}

gboolean
j_backend_data_create (JBackend* backend, gchar const* namespace, gchar const* path, gpointer* data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_create", "%s, %s, %p", namespace, path, (gpointer)data);
	ret = backend->u.data.create(namespace, path, data);
	j_trace_leave("backend_create");

	return ret;
}

gboolean
j_backend_data_open (JBackend* backend, gchar const* namespace, gchar const* path, gpointer* data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_open", "%s, %s, %p", namespace, path, (gpointer)data);
	ret = backend->u.data.open(namespace, path, data);
	j_trace_leave("backend_open");

	return ret;
}

gboolean
j_backend_data_delete (JBackend* backend, gpointer data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_delete", "%p", data);
	ret = backend->u.data.delete(data);
	j_trace_leave("backend_delete");

	return ret;
}

gboolean
j_backend_data_close (JBackend* backend, gpointer data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_close", "%p", data);
	ret = backend->u.data.close(data);
	j_trace_leave("backend_close");

	return ret;
}

gboolean
j_backend_data_status (JBackend* backend, gpointer data, gint64* modification_time, guint64* size)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(modification_time != NULL, FALSE);
	g_return_val_if_fail(size != NULL, FALSE);

	j_trace_enter("backend_status", "%p, %p, %p", data, (gpointer)modification_time, (gpointer)size);
	ret = backend->u.data.status(data, modification_time, size);
	j_trace_leave("backend_status");

	return ret;
}

gboolean
j_backend_data_sync (JBackend* backend, gpointer data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_sync", "%p", data);
	ret = backend->u.data.sync(data);
	j_trace_leave("backend_sync");

	return ret;
}

gboolean
j_backend_data_read (JBackend* backend, gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(buffer != NULL, FALSE);
	g_return_val_if_fail(bytes_read != NULL, FALSE);

	j_trace_enter("backend_read", "%p, %p, %" G_GUINT64_FORMAT ", %" G_GUINT64_FORMAT ", %p", data, buffer, length, offset, (gpointer)bytes_read);
	ret = backend->u.data.read(data, buffer, length, offset, bytes_read);
	j_trace_leave("backend_read");

	return ret;
}

gboolean
j_backend_data_write (JBackend* backend, gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DATA, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(buffer != NULL, FALSE);
	g_return_val_if_fail(bytes_written != NULL, FALSE);

	j_trace_enter("backend_write", "%p, %p, %" G_GUINT64_FORMAT ", %" G_GUINT64_FORMAT ", %p", data, buffer, length, offset, (gpointer)bytes_written);
	ret = backend->u.data.write(data, buffer, length, offset, bytes_written);
	j_trace_leave("backend_write");

	return ret;
}

gboolean
j_backend_meta_init (JBackend* backend, gchar const* path)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	j_trace_enter("backend_init", "%s", path);
	ret = backend->u.meta.init(path);
	j_trace_leave("backend_init");

	return ret;
}

void
j_backend_meta_fini (JBackend* backend)
{
	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_META);

	j_trace_enter("backend_fini", NULL);
	backend->u.meta.fini();
	j_trace_leave("backend_fini");
}

gboolean
j_backend_meta_batch_start (JBackend* backend, gchar const* namespace, gpointer* batch)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	j_trace_enter("backend_batch_start", "%s, %p", namespace, (gpointer)batch);
	ret = backend->u.meta.batch_start(namespace, batch);
	j_trace_leave("backend_batch_start");

	return ret;
}

gboolean
j_backend_meta_batch_execute (JBackend* backend, gpointer batch)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	j_trace_enter("backend_batch_execute", "%p", batch);
	ret = backend->u.meta.batch_execute(batch);
	j_trace_leave("backend_batch_execute");

	return ret;
}

gboolean
j_backend_meta_put (JBackend* backend, gpointer batch, gchar const* key, bson_t const* value)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	j_trace_enter("backend_put", "%p, %s, %p", batch, key, (gpointer)value);
	ret = backend->u.meta.put(batch, key, value);
	j_trace_leave("backend_put");

	return ret;
}

gboolean
j_backend_meta_delete (JBackend* backend, gpointer batch, gchar const* key)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	j_trace_enter("backend_delete", "%p, %s", batch, key);
	ret = backend->u.meta.delete(batch, key);
	j_trace_leave("backend_delete");

	return ret;
}

gboolean
j_backend_meta_get (JBackend* backend, gchar const* namespace, gchar const* key, bson_t* value)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	j_trace_enter("backend_get", "%s, %s, %p", namespace, key, (gpointer)value);
	ret = backend->u.meta.get(namespace, key, value);
	j_trace_leave("backend_get");

	return ret;
}

gboolean
j_backend_meta_get_all (JBackend* backend, gchar const* namespace, gpointer* iterator)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);

	j_trace_enter("backend_get_all", "%s, %p", namespace, (gpointer)iterator);
	ret = backend->u.meta.get_all(namespace, iterator);
	j_trace_leave("backend_get_all");

	return ret;
}

gboolean
j_backend_meta_get_by_value (JBackend* backend, gchar const* namespace, bson_t const* value, gpointer* iterator)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);

	j_trace_enter("backend_get_by_value", "%s, %p, %p", namespace, (gpointer)value, (gpointer)iterator);
	ret = backend->u.meta.get_by_value(namespace, value, iterator);
	j_trace_leave("backend_get_by_value");

	return ret;
}
gboolean
j_backend_meta_iterate (JBackend* backend, gpointer iterator, bson_t* value)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_META, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	j_trace_enter("backend_iterate", "%p, %p", iterator, (gpointer)value);
	ret = backend->u.meta.iterate(iterator, value);
	j_trace_leave("backend_iterate");

	return ret;
}

/**
 * @}
 **/
