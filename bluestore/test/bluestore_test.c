#include "julea_bluestore.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int
main(int argc, char** argv)
{
    void* store;
    void* coll;
    store = julea_bluestore_init("/tmp/bluestore-test");
    int mkrt = julea_bluestore_mkfs(store);
    printf("mkfs returned %d \n", mkrt);
    int mtrt = julea_bluestore_mount(store);
    printf("mount returned %d \n", mtrt);
    coll = julea_bluestore_create_collection(store);

    julea_bluestore_create(store, coll, "test_object");
    int bw = julea_bluestore_write(store, coll, "test_object", 0, "Test Object Content", 19);
    printf("Bytes written: %d \n", bw);
    char* readback = malloc(sizeof(char) * 21);
    int br = julea_bluestore_read(store, coll, "test_object", 0, &readback, 20);
    printf("Bytes read: %d \n", br);
    printf(readback);
    julea_bluestore_delete(store, coll, "test_object");

    julea_bluestore_umount(store, coll);
    free(readback);
}
