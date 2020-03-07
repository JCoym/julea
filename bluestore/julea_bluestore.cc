#include <glob.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <sys/mount.h>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>

#include "os/ObjectStore.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/BlueFS.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "common/admin_socket.h"
#include "global/global_init.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/coredumpctl.h"

#include "include/unordered_map.h"

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
    store.reset(ObjectStore::create(g_ceph_context, "bluestore", path, NULL));
    store->mkfs();
    store->mount();
    bstore = dynamic_cast<BlueStore*> (store.get());
    cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
    ch = bstore->create_new_collection(cid);
}

void julea_bluestore_mount(const char* path) {
    int poolid = 4373;
    
    store.reset(ObjectStore::create(g_ceph_context, "bluestore", path, NULL));
    bstore = dynamic_cast<BlueStore*> (store.get());
    bstore->mount();

    cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
    ch = bstore->open_collection(cid);
}

void julea_bluestore_umount() {
    if (bstore != NULL){
        bstore->umount();
    }
}

// File operations

void julea_bluestore_create(const char* name) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, obj);
    queue_transaction(store, ch, std::move(t));
}

void julea_bluestore_delete(const char* name) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.remove(cid, obj);
    queue_transaction(store, ch, std::move(t));
}

int julea_bluestore_write(const char* name, uint64_t offset, const char* data, uint64_t length) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append(data);
    t.create_collection(cid, 0);
    t.write(cid, obj, offset, length, bl);
    return queue_transaction(store, ch, std::move(t));
}

int julea_bluestore_read(const char* name, uint64_t offset, char* data_read, uint64_t length) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    bufferlist readback;
    int ret = store->read(ch, obj, offset, length, readback);
    data_read = readback.c_str();
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
