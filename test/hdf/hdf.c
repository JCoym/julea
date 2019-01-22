#include <assert.h>
#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>

#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <hdf5/jhdf5.h>

#include "test.h"

#define H5FILE_NAME "Test.h5"
#define DATASETNAME "IntArray"

/* dataset dimensions */
#define NX 5
#define NY 6
#define RANK 2

static void create_random_attribute(hid_t location) {
    /* Create the data space for the attribute. */
    int attr_data[3]; /* attribute data */
    hsize_t dims[1];
    hid_t did;
    hid_t aid;

    /* Initialize the attribute data */
    attr_data[0] = 20;
    attr_data[1] = 30;
    attr_data[2] = 50;

    dims[0] = 3;
    did = H5Screate_simple(1, dims, NULL);

    /* Create a dataset attribute. */
    aid = H5Acreate2(location, "Units", H5T_NATIVE_INT, did, H5P_DEFAULT,
                     H5P_DEFAULT);

    /* Write the attribute data. */
    H5Awrite(aid, H5T_NATIVE_INT, attr_data);

    H5Sclose(did);
    H5Aclose(aid);
}

static void create_random_dataset(hid_t location, const char *name) {
    hid_t dataset; /* file and dataset handles */
    hid_t datatype, dataspace;
    herr_t status __attribute__((unused));
    hsize_t dimsf[2]; /* dataset dimensions */
    int data[NX][NY];

    /* Define datatype for the data in the file*/
    datatype = H5Tcopy(H5T_NATIVE_INT);
    status = H5Tset_order(datatype, H5T_ORDER_LE);

    /* Describe the size of the array and create the data space for fixed size
     * dataset */
    dimsf[0] = NX;
    dimsf[1] = NY;
    dataspace = H5Screate_simple(RANK, dimsf, NULL);

    /* Create a new dataset within the file using defined dataspace */
    dataset = H5Dcreate2(location, name, datatype, dataspace, H5P_DEFAULT,
                         H5P_DEFAULT, H5P_DEFAULT);

    /* Data  and output buffer initialization */

    for (size_t j = 0; j < NX; j++)
        for (size_t i = 0; i < NY; i++)
            data[j][i] = i + j;

    /* Write the data to the dataset using default transfer properties */
    H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

    create_random_attribute(dataset);

    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);
}

static void test_hdf_write(void) {
    hid_t fid;
    hid_t under_fapl;
    hid_t acc_tpl;
    hid_t vol_id;
    const H5VL_class_t *h5vl_log;
    hid_t native_plugin_id;

    /* set native VOL plugin */
    under_fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_native(under_fapl);
    g_assert(H5VLis_registered("native") == 1);

    h5vl_log = H5PLget_plugin_info();
    vol_id = H5VLregister(h5vl_log);
    g_assert(vol_id > 0);
    g_assert(H5VLis_registered("extlog") == 1);

    native_plugin_id = H5VLget_plugin_id("native");
    g_assert(native_plugin_id > 0);

    /* set extern VOL plugin */
    acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_vol(acc_tpl, vol_id, &under_fapl);

    /* Create a new file  */
    fid = H5Fcreate(H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, acc_tpl);

    create_random_dataset(fid, DATASETNAME);

    H5Fclose(fid);
    H5Pclose(acc_tpl);
    H5Pclose(under_fapl);
    H5VLclose(native_plugin_id);
    H5VLterminate(vol_id, H5P_DEFAULT);
    H5VLunregister(vol_id);

    g_assert(H5VLis_registered("extlog") == 0);
}

static void test_hdf_read(void) {
    hid_t under_fapl;
    hid_t acc_tpl;
    hid_t vol_id, vol_id2;

    hid_t file, dataset; /* File and dataset handles */
    int *data_out;       /* Buffer to read attribute back */
    herr_t status __attribute__((unused));

    hid_t attr, atype, aspace; /* Attribute, datatype and dataspace identifiers */
    int rank;
    hsize_t sdim[64];

    size_t npoints; /* Number of elements in the array attribute. */

    const H5VL_class_t *h5vl_log;
    hid_t native_plugin_id;

    hid_t mem_type_id;
    size_t type_size;
    hid_t mem_space_id;
    hssize_t n_data_points;
    void *buf;

    /* set native VOL plugin */
    under_fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_native(under_fapl);
    g_assert(H5VLis_registered("native") == 1);

    h5vl_log = H5PLget_plugin_info();
    vol_id = H5VLregister(h5vl_log);
    g_assert(vol_id > 0);
    g_assert(H5VLis_registered("extlog") == 1);

    vol_id2 = H5VLget_plugin_id("extlog");
    H5VLinitialize(vol_id2, H5P_DEFAULT);
    H5VLclose(vol_id2);

    native_plugin_id = H5VLget_plugin_id("native");
    g_assert(native_plugin_id > 0);

    /* set extern VOL plugin */
    acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_vol(acc_tpl, vol_id, &under_fapl);

    /* Open the file */
    file = H5Fopen("Test.h5", H5F_ACC_RDONLY, acc_tpl);

    /* Open the dataset */
    dataset = H5Dopen2(file, "IntArray", H5P_DEFAULT);
    /* Testing "open"-function: Open attribute using its name, then display
     * attribute name*/
    attr = H5Aopen(dataset, "Units", H5P_DEFAULT);

    /* Testing "get"-functions: Get attribute datatype, dataspace, rank, and
     * dimensions*/
    atype = H5Aget_type(attr);
    aspace = H5Aget_space(attr);
    rank = H5Sget_simple_extent_ndims(aspace);
    status = H5Sget_simple_extent_dims(aspace, sdim, NULL);

    /* Display datatype for attribute */
    if (H5T_FLOAT == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_FLOAT \n");}
    if (H5T_INTEGER == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_INTEGER \n");}
    if (H5T_STRING == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_STRING \n");}
    if (H5T_BITFIELD == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_BITFIELD \n");}
    if (H5T_OPAQUE == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_OPAQUE \n");}
    if (H5T_COMPOUND == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_COMPOUND \n");}
    if (H5T_REFERENCE == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_REFERENCE \n");}
    if (H5T_ENUM == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_ENUM \n");}
    if (H5T_VLEN == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_VLEN \n");}
    if (H5T_ARRAY == H5Tget_class(atype)) {
    } // printf("DATATYPE : H5T_ARRAY \n");}

    /* Display dataspace for attribute */
    if (H5Sis_simple(aspace)) {
    } // printf("DATASPACE : SIMPLE \n");}
    else {
    } // printf("DATASPACE : SCALAR \n");}

    npoints = H5Sget_simple_extent_npoints(aspace);
    data_out = (int *)malloc(sizeof(int) * (int)npoints);

    status = H5Aread(attr, H5T_NATIVE_INT, data_out);
    free(data_out);

    mem_type_id = H5Dget_type(dataset);
    type_size = H5Tget_size(mem_type_id);
    mem_space_id = H5Dget_space(dataset);
    n_data_points = H5Sget_simple_extent_npoints(mem_space_id);
    buf = malloc(type_size * n_data_points);
    H5Dread(dataset, mem_type_id, mem_space_id, H5S_ALL, H5P_DEFAULT, buf);

    free(buf);

    status = H5Aclose(attr);
    status = H5Dclose(dataset);
    status = H5Fclose(file);

    status = H5Pclose(acc_tpl);
    status = H5Pclose(under_fapl);
    status = H5VLclose(native_plugin_id);
    status = H5VLterminate(vol_id, H5P_DEFAULT);
    status = H5VLunregister(vol_id);
    g_assert(H5VLis_registered("extlog") == 0);
}

void test_hdf(void) {
    g_test_add_func("/hdf/hdf/write", test_hdf_write);
    g_test_add_func("/hdf/hdf/read", test_hdf_read);
}