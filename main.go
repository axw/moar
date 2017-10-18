package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mongodb/mongo-tools/common/archive"
)

var dir = flag.String("d", "", "Directory to which contents will be written")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n  %s [options] <archive>:\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func Main(archivePath string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := newArchiveReader(f)
	_, meta, err := r.ReadMetadata()
	if err != nil {
		return err
	}
	if err := r.ReadCollections(meta, getHandler); err != nil {
		return err
	}
	return nil
}

func getHandler(meta *archive.CollectionMetadata) (archive.DemuxOut, error) {
	if meta.Database != "juju" {
		return nil, nil
	}
	suffix := ".bson"
	filename := fmt.Sprintf("%s.%s%s", meta.Database, meta.Collection, suffix)
	if dir != nil {
		filename = filepath.Join(*dir, filename)
	}
	log.Println("writing", filename)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	d := newDemuxOut(f)
	return d, nil
}

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "ERROR: missing path to mongodump archive\n")
		flag.Usage()
		os.Exit(2)
	}
	if err := Main(flag.Arg(0)); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
