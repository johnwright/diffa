all: install-protobuf

protobuf-2.3.0.tar.gz:
	wget http://protobuf.googlecode.com/files/protobuf-2.3.0.tar.gz
protobuf-2.3.0: | protobuf-2.3.0.tar.gz
	tar -xzf protobuf-2.3.0.tar.gz
protobuf-2.3.0/Makefile: | protobuf-2.3.0
	(cd protobuf-2.3.0 && ./configure --prefix=/usr)
protobuf-2.3.0/src/protoc: protobuf-2.3.0/Makefile
	(cd protobuf-2.3.0 && make && make check)
install-protobuf: protobuf-2.3.0/src/protoc
	(cd protobuf-2.3.0 && sudo make install)
