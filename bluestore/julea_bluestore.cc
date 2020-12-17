#include <stdio.h>
#include <string.h>

#include "os/ObjectStore.h"
#include "os/bluestore/BlueStore.h"
#include "global/global_init.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"

boost::intrusive_ptr<ceph::common::CephContext> cct;

typedef struct BSColl
{
    coll_t cid;
    ObjectStore::CollectionHandle ch;
} BSColl;

ghobject_t make_object(const char* name, int64_t pool) {
    sobject_t soid{name, CEPH_NOSNAP};
    uint32_t hash = std::hash<sobject_t>{}(soid);
    return ghobject_t{hobject_t{soid, "", hash, pool, ""}};
}

#ifdef __cplusplus
extern "C" {
#endif

// BlueStore operations

void *julea_bluestore_init(const char* path) {
    vector<const char*> args;
    ObjectStore* store;
    cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);
    common_init_finish(g_ceph_context);
    store = ObjectStore::create(g_ceph_context, string("bluestore"), string(path), string("store_temp_journal"));
    return (void *)store;
}

int julea_bluestore_mkfs(void* store) {
    ObjectStore* ostore = (ObjectStore *)store;
    return ostore->mkfs();
}

int julea_bluestore_mount(void* store) {
    ObjectStore* ostore = (ObjectStore *)store;
    return ostore->mount();
}

void *julea_bluestore_create_collection(void* store) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = new BSColl;
    coll->cid = coll_t();
    coll->ch = ostore->create_new_collection(coll->cid);
    {
        BlueStore::Transaction t;
        t.create_collection(coll->cid, 0);
        ostore->queue_transaction(coll->ch, std::move(t));
    }
    return (void *)coll;
}

void *julea_bluestore_open_collection(void* store) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = new BSColl;
    coll->cid = coll_t();
    coll->ch = ostore->open_collection(coll->cid);
    return (void *)coll;
}

int julea_bluestore_umount(void* store, void* bscoll) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = (BSColl *)bscoll;
    int ret = 0;
    if (ostore != NULL){
        coll->ch.reset();
        ret = ostore->umount();
    }
    delete coll;
    return ret;
}

// File operations

int julea_bluestore_create(void* store, void* bscoll, const char* name) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = (BSColl *)bscoll;
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    t.touch(coll->cid, obj);
    return ostore->queue_transaction(coll->ch, std::move(t));
}

int julea_bluestore_delete(void* store, void* bscoll, const char* name) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = (BSColl *)bscoll;
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    t.remove(coll->cid, obj);
    return ostore->queue_transaction(coll->ch, std::move(t));
}

int julea_bluestore_write(void* store, void* bscoll, const char* name, uint64_t offset, const char* data, uint64_t length) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = (BSColl *)bscoll;
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append(string(data));
    t.write(coll->cid, obj, offset, bl.length(), bl);
    ostore->queue_transaction(coll->ch, std::move(t));
    return bl.length();
}

int julea_bluestore_read(void* store, void* bscoll, const char* name, uint64_t offset, char** data_read, uint64_t length) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = (BSColl *)bscoll;
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    bufferlist readback;
    int ret = ostore->read(coll->ch, obj, offset, length, readback);
    *data_read = readback.c_str();
    return ret;
}

int julea_bluestore_status(void* store, void* bscoll, const char* name, struct stat* st) {
    ObjectStore* ostore = (ObjectStore *)store;
    BSColl* coll = (BSColl *)bscoll;
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    return ostore->stat(coll->ch, obj, st);
}

#ifdef __cplusplus
}
#endif
