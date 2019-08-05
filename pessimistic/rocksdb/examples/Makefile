include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

.PHONY: clean librocksdb

all: test_skiplistInsert_atomicity_pcc

test_skiplistInsert_atomicity_pcc: librocksdb test_skiplistInsert_atomicity_pcc.cc 
	# $(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb_debug.a -I../include -O2 -std=c++14 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) # this was for all our assertations to be enabled
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++14 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)


clean:
	rm -rf ./test_skiplistInsert_atomicity_pcc

librocksdb:
	cd .. && $(MAKE) static_lib -j 20
	# cd .. && $(MAKE) all -j20
