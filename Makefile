REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all l3vpn-processor container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: l3vpn-processor

l3vpn-processor:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-l3vpn-processor

l3vpn-processor-container: l3vpn-processor
	docker build -t $(REGISTRY_NAME)/l3vpn-processor:$(IMAGE_VERSION) -f ./build/Dockerfile.l3vpn-processor .

push: l3vpn-processor-container
	docker push $(REGISTRY_NAME)/l3vpn-processor:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
