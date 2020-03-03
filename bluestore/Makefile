CC = g++

# define any compile-time flags
CFLAGS = -std=c++17

# define any directories containing header files other than /usr/include
#
INCLUDES = -I../dependencies/ceph/src -I../dependencies/ceph/build/include -I../dependencies/ceph/build/boost/include

# define library paths in addition to /usr/lib
#   if I wanted to include libraries not in /usr/lib I'd specify
#   their path using -Lpath, something like:
LFLAGS = -L../dependencies/ceph/build/lib -L../dependencies/ceph/build/src/rocksdb

# define any libraries to link into executable:
#   if I want to link in libraries (libx.so or libx.a) I use the -llibname 
#   option, something like (this will link in libmylib.so and libm.so:
LIBS = -lceph-common -lbluefs -lcommon -lkv -lpthread -lrocksdb -lz -lceph_lz4 -llz4 -los -lcrypto -ldl

# define the C source files
SRCS = julea_bluestore.cc

OBJS = $(SRCS:.cc=.o)

# define the executable file 
MAIN = julea_bluestore

.PHONY: clean

all:    $(MAIN)

$(MAIN): $(OBJS) 
		$(CC) $(CFLAGS) -fPIC $(INCLUDES) -o $(MAIN) $(OBJS) $(LFLAGS) $(LIBS)

.cc.o:
		$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
		$(RM) *.o *~ $(MAIN)
