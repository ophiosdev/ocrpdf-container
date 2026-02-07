IMAGE ?= ocrpdf:dev

.PHONY: build clean

build:
	docker build -t $(IMAGE) .

clean:
	docker image rm -f $(IMAGE)
