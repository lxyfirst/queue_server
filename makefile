
SUBDIRS=framework pugixml jsoncpp public queue_server

all: 
	@for DIR in $(SUBDIRS) ; do 	\
		make all -C $$DIR ;         \
	done

release:
	@for DIR in $(SUBDIRS) ; do 	\
		make release -C $$DIR ; 	\
	done

clean:
	@for DIR in $(SUBDIRS) ; do 	\
		make clean -C $$DIR ; 		\
	done

install:
	@if ! [ -e deploy ] ; then mkdir deploy ; fi
	cp -f queue_server/*.bin deploy/
	cp -f queue_server/*.xml deploy/
