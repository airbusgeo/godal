all: build-gs build-s3

build-gs:
	go build -o gdal_gcs.so -buildmode=c-shared gcs_adapter.go

build-s3:
	go build -o gdal_s3.so -buildmode=c-shared s3_adapter.go

GDALCONFIG := $(shell command -v gdal-config 2> /dev/null)

install: install-gs install-s3

install-gs:
ifndef GDALCONFIG
	@echo "Could not find the gdal-config utility. Installation failed."
	@echo 'Manually copy gdal_gcs.so to your $$(gdal-config --prefix)/lib/gdalplugins directory.'
else
	$(eval PLUGIN_DIR=$(shell gdal-config --prefix)/lib/gdalplugins)
	cp gdal_gcs.so $(PLUGIN_DIR)/gdal_gcs.so
endif

install-s3:
ifndef GDALCONFIG
	@echo "Could not find the gdal-config utility. Installation failed."
	@echo 'Manually copy gdal_s3.so to your $$(gdal-config --prefix)/lib/gdalplugins directory.'
else
	$(eval PLUGIN_DIR=$(shell gdal-config --prefix)/lib/gdalplugins)
	cp gdal_s3.so $(PLUGIN_DIR)/gdal_s3.so
endif


