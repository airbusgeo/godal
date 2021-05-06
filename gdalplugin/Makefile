all: build

build:
	go build -o gdal_gcs.so -buildmode=c-shared gcs_adapter.go

GDALCONFIG := $(shell command -v gdal-config 2> /dev/null)

install:
ifndef GDALCONFIG
	@echo "Could not find the gdal-config utility. Installation failed."
	@echo 'Manually copy gdal_gcs.so to your $$(gdal-config --prefix)/lib/gdalplugins directory.'
else
	$(eval PLUGIN_DIR=$(shell gdal-config --prefix)/lib/gdalplugins)
	cp gdal_gcs.so $(PLUGIN_DIR)/gdal_gcs.so
endif

