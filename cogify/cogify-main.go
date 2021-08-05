package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/airbusgeo/cogger"
	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/osio"
	"github.com/spf13/cobra"
)

func gsparse(file string) (bucket, object string) {
	if !strings.HasPrefix(file, "gs://") {
		return
	}
	file = file[5:]
	firstSlash := strings.Index(file, "/")
	if firstSlash == -1 {
		return
	}
	obj := strings.Trim(file[firstSlash:], "/")
	if obj == "" {
		return
	}
	bucket = file[0:firstSlash]
	object = obj
	return
}

var outfile string
var blockSize string
var numCachedBlocks int //= flag.Int("gs.numblocks", 512, "osio number of cached blocks")
var tmpdir string       //= flag.String("tmpdir", ".", "temporary directory for intermediate file")
var overviews bool      //= flag.Bool("ovr", true, "compute overviews")

func init() {
	cogCommand.Flags().StringVarP(&blockSize, "gs.blocksize", "b", "512k", "gs:// block size")
	cogCommand.Flags().IntVarP(&numCachedBlocks, "gs.numblocks", "n", 512, "number of gs:// blocks to cache")
	cogCommand.Flags().StringVar(&tmpdir, "tmp", ".", "directory to use for temp file")
	cogCommand.Flags().BoolVar(&overviews, "ovr", true, "compute overviews")
	cogCommand.Flags().StringVarP(&outfile, "out", "o", "out-cog.tif", "output cog name")
}
func main() {
	err := cogCommand.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var cogCommand = &cobra.Command{
	Use:   "cogify [flags] -- infile [gdal switches]*",
	Short: "convert a generic tiff to COG",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		infile := args[0]
		args = args[1:]
		ib, _ := gsparse(infile)
		ob, oo := gsparse(outfile)
		var stcl *storage.Client
		var err error
		if ib != "" || ob != "" {
			stcl, err = storage.NewClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create gcs storage client: %w", err)
			}
			gs, err := osio.GCSHandle(ctx, osio.GCSClient(stcl))
			if err != nil {
				return fmt.Errorf("osio.gcshandle: %w", err)
			}
			gsa, err := osio.NewAdapter(gs, osio.BlockSize(blockSize), osio.NumCachedBlocks(numCachedBlocks))
			if err != nil {
				return fmt.Errorf("osio.newadapter: %w", err)
			}
			err = godal.RegisterVSIAdapter("gs://", gsa)
			if err != nil {
				return fmt.Errorf("godal.registervsi: %w", err)
			}
		}
		godal.RegisterAll()
		inds, err := godal.Open(infile, godal.RasterOnly())
		if err != nil {
			return fmt.Errorf("open %s: %w", infile, err)
		}
		if len(args) == 0 {
			args = []string{
				"-co", "BLOCKXSIZE=256",
				"-co", "BLOCKYSIZE=256",
				"-co", "COMPRESS=LZW",
			}
		}
		args = append(args,
			"-co", "TILED=YES",
			"-co", "BIGTIFF=YES",
			"-of", "GTiff",
		)
		tmpf, err := ioutil.TempFile(tmpdir, "*.tif")
		if err != nil {
			return err
		}
		tmpf.Close()
		tmpfname := tmpf.Name()
		defer os.Remove(tmpfname)

		outds, err := inds.Translate(tmpfname, args)
		if err != nil {
			return fmt.Errorf("translate: %w", err)
		}
		if overviews {
			err = outds.BuildOverviews()
			if err != nil {
				return fmt.Errorf("build overviews: %w", err)
			}
		}
		err = outds.Close()
		if err != nil {
			return fmt.Errorf("close temp tif: %w", err)
		}

		tmpf, err = os.Open(tmpfname)
		if err != nil {
			return fmt.Errorf("re-open temp tif %s: %w", tmpfname, err)
		}
		defer tmpf.Close()

		var outr io.WriteCloser
		if ob == "" {
			outr, err = os.Create(outfile)
			if err != nil {
				return fmt.Errorf("create %s: %w", outfile, err)
			}
		} else {
			outr = stcl.Bucket(ob).Object(oo).NewWriter(ctx)
		}

		err = cogger.Rewrite(outr, tmpf)
		if err != nil {
			return fmt.Errorf("cogger.rewrite: %w", err)
		}

		err = outr.Close()
		if err != nil {
			return fmt.Errorf("close %s: %w", outfile, err)
		}
		return nil
	},
}
