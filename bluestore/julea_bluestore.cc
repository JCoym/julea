#include <stdio.h>
#include <string.h>

#include "os/ObjectStore.h"
#include "os/bluestore/BlueStore.h"
#include "global/global_init.h"

using namespace std::placeholders;

boost::scoped_ptr<ObjectStore> store;
BlueStore *bstore;
coll_t cid;
ObjectStore::CollectionHandle ch;

ghobject_t make_object(const char* name, int64_t pool) {
    sobject_t soid{name, CEPH_NOSNAP};
    uint32_t hash = std::hash<sobject_t>{}(soid);
    return ghobject_t{hobject_t{soid, "", hash, pool, ""}};
}

template <typename T>
int queue_transaction(T &store, ObjectStore::CollectionHandle ch, ObjectStore::Transaction &&t) {
  if (rand() % 2) {
    ObjectStore::Transaction t2;
    t2.append(t);
    return store->queue_transaction(ch, std::move(t2));
  } else {
    return store->queue_transaction(ch, std::move(t));
  }
}

#ifdef __cplusplus
extern "C" {
#endif

// BlueStore operations

void julea_bluestore_init(const char* path) {
    int poolid = 4373;
    vector<const char*> args;
    auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);
    common_init_finish(g_ceph_context);
    store.reset(ObjectStore::create(g_ceph_context, string("bluestore"), string(path), string("")));
    bstore = dynamic_cast<BlueStore*> (store.get());
    store->mkfs();
    store->mount();

    cid = coll_t(spg_t());
    ch = bstore->create_new_collection(cid);
    {
        BlueStore::Transaction t;
        t.create_collection(cid, 0);
        bstore->queue_transaction(ch, std::move(t));
    }
}

void julea_bluestore_mount(const char* path) {
    int poolid = 4373;
    vector<const char*> args;
    auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);
    common_init_finish(g_ceph_context);
    store.reset(ObjectStore::create(g_ceph_context, string("bluestore"), string(path), string("")));
    bstore = dynamic_cast<BlueStore*> (store.get());
    bstore->mount();

    cid = coll_t(spg_t());
    ch = bstore->open_collection(cid);
}

void julea_bluestore_umount() {
    if (bstore != NULL){
        ch.reset();
        bstore->umount();
    }
}

// File operations

void julea_bluestore_create(const char* name) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    t.touch(cid, obj);
    int r = bstore->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
}

void julea_bluestore_delete(const char* name) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    t.remove(cid, obj);
    int r = queue_transaction(store, ch, std::move(t));
    ceph_assert(r == 0);
}

int julea_bluestore_write(const char* name, uint64_t offset, const char* data, uint64_t length) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append(string(data));
    t.write(cid, obj, offset, bl.length(), bl);
    queue_transaction(store, ch, std::move(t));
    return bl.length();
}

int julea_bluestore_read(const char* name, uint64_t offset, char** data_read, uint64_t length) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    bufferlist readback;
    int ret = store->read(ch, obj, offset, length, readback);
    *data_read = readback.c_str();
    printf(readback.c_str());
    return ret;
}

int julea_bluestore_status(const char* name, struct stat* st) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    return store->stat(ch, obj, st);
}

#ifdef __cplusplus
}
#endif
