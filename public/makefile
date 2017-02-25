

SRCS=$(wildcard *.cpp)
PROTO_SRCS=$(wildcard *.pb.cc)

DEPS=$(SRCS:.cpp=.d) 
OBJS=$(SRCS:.cpp=.o) $(PROTO_SRCS:.cc=.o)

LIB_OBJS=$(SRCS:.cpp=.o) $(PROTO_SRCS:.cc=.o) 
BINS=$(patsubst %.cpp,%,$(BIN_SRCS))

LIB_NAME=proto_message
SO_LIB=$(patsubst %,lib%.so,$(LIB_NAME))
STATIC_LIB=$(patsubst %,lib%.a,$(LIB_NAME))


CC=clang++
#CC=clang++
CFLAGS= -Wall -D_REENTRANT -D_GNU_SOURCE -fPIC
#CFLAGS += -std=c++11
BOOST_DIR = /usr/local/include/boost/
#INC= -I${BOOST_DIR}  -I. -I../public 
INC= -I. -I..

ifeq ($(release), 1)
CFLAGS += -O2 -DNDEBUG 
else
CFLAGS += -g -DDEBUG 
endif

all: $(BINS) $(SO_LIB) $(STATIC_LIB) 

$(BINS): % : %.o $(LIB_OBJS) 
	$(CC) $(CFLAGS) -o $@ $^  
$(SO_LIB): $(LIB_OBJS) 
	$(CC) $(CFLAGS) -shared -o $@ $^ 
$(STATIC_LIB): $(LIB_OBJS) 
	ar -crs  $@ $^ 
dist :
	rm -f $(DEPS)
clean:
	rm -f $(BINS) $(OBJS) $(DEPS) $(SO_LIB) $(STATIC_LIB)
release: clean
	@make release=1

%.d : %.cpp 
	$(CC) $(CFLAGS) -MT $(subst .cpp,.o,$<) -MM $(INC)  $< >$@
%.o : %.cpp 
	$(CC) $(CFLAGS) $(INC) -c  $< 
%.pb.o : %.pb.cc
	$(CC) $(CFLAGS) $(INC) -c  $< 

proto : 
	@if $(CC) -S *.pb.h -o /dev/null >/dev/null 2>&1 ;  \
	then  \
        echo "proto files already exists"   ;\
	else  \
		protoc --cpp_out=. *.proto   ;\
        echo "generate proto files successfully"  ;\
	fi

-include  $(DEPS)

