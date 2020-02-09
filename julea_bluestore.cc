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
#include <gtest/gtest.h>

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
    auto cct;

julea_bluestore_create(const string& path) {
    cct = global_init(NULL, NULL, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, NULL);
    store = ObjectStore(cct, path);
    store.create(cct, "bluestore", path, "", NULL);
    bstore = dynamic_cast<BlueStore*> (store.get());
    cid = coll_t();
}

julea_bluestore_mount(const string& path) {
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

    cid = coll_t();
}

julea_bluestore_umount() {
    if (cct != NULL && bstore != NULL){
        bstore->umount();
    }
}

julea_bluestore_create() {
    // TODO
}

julea_bluestore_delete() {
    // TODO
}

julea_bluestore_write() {
    // TODO
}

julea_bluestore_read() {
    // TODO
}

julea_bluestore_status() {
    // TODO
}