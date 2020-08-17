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
    mkdir("/tmp/bluestore-test/", S_IRUSR | S_IWUSR | S_IXUSR);
    julea_bluestore_init("/tmp/ceph-data");
    julea_bluestore_create("test_object");
    int bw = julea_bluestore_write("test_object", 0, "Test Object Content", 20);
    char* readback = malloc(sizeof(char) * 21);
    int br = julea_bluestore_read("test_object", 0, readback, 20);
    printf(readback);
    julea_bluestore_umount();
    free(readback);
}
