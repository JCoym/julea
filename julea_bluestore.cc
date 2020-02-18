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

public:
    ObjectStore *store;
    BlueStore* bstore
    coll_t cid;
    CollectionHandle ch;
    auto cct;

ghobject_t make_object(const char* name, int64_t pool) {
    sobject_t soid{name, CEPH_NOSNAP};
    uint32_t hash = std::hash<sobject_t>{}(soid);
    return ghobject_t{hobject_t{soid, "", hash, pool, ""}};
}

// BlueStore operations

void julea_bluestore_init(const string& path) {
    int poolid = 4373;
    cct = global_init(NULL, NULL, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, NULL);
    store = ObjectStore(cct, path);
    store.create(cct, "bluestore", path, "", NULL);
    bstore = dynamic_cast<BlueStore*> (store.get());
    cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
    ch = bstore->create_new_collection(cid);
}

void julea_bluestore_mount(const string& path) {
    int poolid = 4373;

    if (cct == NULL) {
        cct = global_init(NULL, NULL, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, NULL);
    }

    if (store == NULL) {
        store = ObjectStore(cct, path);
    }

    if (bstore == NULL) {
        bstore = dynamic_cast<BlueStore*> (store.get());
    }
    else {
        bstore->mount();
    }

    cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
    ch = bstore->open_collection(cid);
}

void julea_bluestore_umount() {
    if (cct != NULL && bstore != NULL){
        bstore->umount();
    }
}

// File operations

void julea_bluestore_create(const char* name) {
    const uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("");
    t.create_collection(cid, 0);
    t.write(cid, obj, 0, 0, bl);
    queue_transaction(store, ch, std::move(t));
}

void julea_bluestore_delete() {
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
    onst uint64_t pool = 4373;
    ghobject_t obj = make_object(name, pool);
    bufferlist readback;
    int ret = store->read(ch, obj, offset, length, readback);
    bl.copy_out(0, length, data_read)
    return ret;
}

void julea_bluestore_status() {
    // TODO
}