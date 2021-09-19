CONFIG_PATH=${HOME}/.proglog/

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf
$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	# Creates Certificate Authority files.
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
	# Creates the server certificate.
	cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=test/ca-config.json \
	-profile=server \
	test/server-csr.json | cfssljson -bare server
	# Creates the client root certificate.
	cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=test/ca-config.json \
	-profile=client \
	-cn="root" \
	test/client-csr.json | cfssljson -bare root-client
	# Creates the certificate for the client with no permissions to do anything.
	cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=test/ca-config.json \
	-profile=client \
	-cn="client" \
	test/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	protoc api/v1/*.proto \
    --go_out=. \
    --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    --proto_path=.
.PHONY: test
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race ./...