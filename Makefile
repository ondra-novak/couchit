ifeq "$(MAKECMDGOALS)" "runtests"
	FORCE_DEBUG:=0
	APPNAME:=bin/tests
	SOURCES:=src
	BUILDTYPE=app
	LDOTHERLIBS:=-lpthread
else 
ifeq "$(MAKECMDGOALS)" "debugtests"
	FORCE_DEBUG:=1
	APPNAME:=bin/tests
	SOURCES:=src
	BUILDTYPE=app
	LDOTHERLIBS:=-lpthread
else
	LIBNAME:=lightcouch
	BUILDTYPE=lib
	SOURCES=src/lightcouch
endif
endif

LIBINCLUDES:=src
SEARCHPATHS="./ ../ ../../ /usr/include/ /usr/local/include/"
CXX=clang++
CXXFLAGS=-std=c++11

selectpath=$(abspath $(firstword $(foreach dir,$(1),$(wildcard $(dir)$(2)))))

LIBLIGHTSPEED=$(call selectpath,$(SEARCHPATHS),lightspeed)
NEEDLIBS:=$(LIBLIGHTSPEED)
LIBJSONRPCSERVER=$(call selectpath,$(SEARCHPATHS),jsonrpcserver)
NEEDLIBS+=$(LIBJSONRPCSERVER)
 
 
include $(LIBLIGHTSPEED)/building/build_$(BUILDTYPE).mk

runtests: $(APPNAME) 
	./$(APPNAME)
	
debugtests: $(APPNAME) 
	
	