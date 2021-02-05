#!/bin/bash

GDALVERSION=$1

apt-get install -y libproj-dev libgeos-dev build-essential libsqlite3-dev curl pkgconf libjpeg-turbo8-dev sqlite3
cd $HOME
mkdir -p gdal
cd gdal

curl -sL https://github.com/OSGeo/gdal/archive/$GDALVERSION.tar.gz -o gdal.tar.gz
mkdir gdal
tar  xzf gdal.tar.gz -C gdal --strip-components 1
cd gdal/gdal
./configure --prefix=/optgdal \
	--disable-static \
	--enable-shared \
	--disable-static \
	--disable-all-optional-drivers \
	--with-geos \
	--with-jpeg \
	--with-libtiff=internal \
	--with-geotiff=internal \
	--with-geos \
	--without-crypto \
	--without-cryptopp \
	--without-gnm \
	--without-qhull \
	--without-sqlite3 \
	--without-pcidsk \
	--without-lerc  \
	--without-gif \
	--without-pcraster \
	--without-curl \
	--without-png \
	--without-tiledb \
	--without-odbc \
	--without-freexl \
	--without-pcre \
	--without-libkml \
	--without-xml2 \
	--without-expat \
	--without-xerces \
	--without-lerc \
	--without-pg \
	--without-curl \
	--without-openjpeg \
	--without-netcdf \
	--without-hdf5 \
	--without-hdf4 \
	--without-ogdi \
	--without-exr \
	--without-spatialite

make -j4
make install
cd $HOME
rm -rf gdal
rm -rf /usr/local/bin
