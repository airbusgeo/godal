# VSI gdal plugin

This folder contains the sources required to build a gdal plugin that will make
gdal use the `cloud.google.com/go/storage` SDK to access any file starting with `gs://`
passed to `GDALOpen`. Once the `gdal_gcs.so` file has been copied to
`$(GDAL_INSTALL_PREFIX)/lib/gdalplugins/` any program linked to gdal
(e.g. gdalinfo, gdal_translate, qgis, etc...) will be able to open `gs://bucket/path/to/cog.tif`
files directly.

### Caveats

* This plugin does not work with musl, and therefore cannot be run on alpine like
  linux distrbutions ( waiting on https://github.com/golang/go/issues/13492 ).
* The internal cache configuration can be tweaked before program startup with the
  `GODAL_BLOCKSIZE` and `GODAL_NUMBLOCKS` environment variables. The default is to cache
  1000 bocks of 1Mb. `GODAL_BLOCKSIZE` may be expressed either as a number of bytes, or a number
  suffixed with Kb or Mb.
* Although any file format can be accessed through this handler, I/O performance will only
  be reasonable for "cloud optimized" formats (namely COGs).
* If you only need to access `gs://` files from a golang/godal program, it is preferrable to
  use `godal.RegisterGCSHandler()` directly rather than using this plugin in order to
  fine tune the handler's configuration. In that case, the call to `RegisterGCSHandler()`
  should be done before the call to `RegisterAll()` to prevent the plugin from registering
  the `gs://` prefix.
* Authentication is handled entirely by the `cloud.google.com/go/storage` SDK, which should be 
  transparent on any GCP compute instance. If running outside of GCP, you should launch
  `gcloud auth application-default login` to setup your credentials.

### Building

From this directory, run `make && sudo make install` . If the installation cannot be
performed automatically, manually copy `gdal_gcs.so` to your `$(GDAL_INSTALL_PREFIX)/lib/gdalplugins`
folder.