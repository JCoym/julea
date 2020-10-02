#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

void *julea_bluestore_init(const char*);

int julea_bluestore_mkfs(void*);

int julea_bluestore_mount(void*);

void *julea_bluestore_create_collection(void*);

void *julea_bluestore_open_collection(void*);

void julea_bluestore_umount(void*, void*);

void julea_bluestore_create(void*, void*, const char*);

void julea_bluestore_delete(void*, void*, const char*);

int julea_bluestore_write(void*, void*,const char*, uint64_t, const char*, uint64_t);

int julea_bluestore_read(void*, void*,const char*, uint64_t, char**, uint64_t);

int julea_bluestore_status(void*, void*,const char*, struct stat*);

#ifdef __cplusplus
}
#endif
