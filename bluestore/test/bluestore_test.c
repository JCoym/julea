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
    //printf("check");
    //init_bs_folder("/tmp/bluestore-test", "/dev/sdb");
    //julea_bluestore_init("/tmp/bluestore-test");
    julea_bluestore_mount("/tmp/bluestore-test");
    julea_bluestore_create("test_object");
    int bw = julea_bluestore_write("test_object", 0, "Test Object Content", 19);
    printf("%d \n", bw);
    char* readback = malloc(sizeof(char) * 21);
    int br = julea_bluestore_read("test_object", 0, &readback, 20);
    printf("%d \n", br);
    printf(readback);
    julea_bluestore_umount();
    free(readback);
}
