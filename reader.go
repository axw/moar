package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc64"
	"io"

	"github.com/mongodb/mongo-tools/common/archive"
	"gopkg.in/mgo.v2/bson"
)

type archiveReader struct {
	r *bufio.Reader
}

func newArchiveReader(r io.Reader) *archiveReader {
	return &archiveReader{
		bufio.NewReader(r),
	}
}

func (r *archiveReader) ReadMetadata() (*archive.Header, []*archive.CollectionMetadata, error) {
	var magic uint32
	if err := binary.Read(r.r, binary.LittleEndian, &magic); err != nil {
		return nil, nil, err
	}
	if magic != archive.MagicNumber {
		return nil, nil, errors.New("not a mongodump archive")
	}
	var consumer preludeParserConsumer
	parser := &archive.Parser{In: r.r}
	if err := parser.ReadBlock(&consumer); err != nil {
		return nil, nil, err
	}
	return &consumer.header, consumer.collections, nil
}

func (r *archiveReader) ReadCollections(
	meta []*archive.CollectionMetadata,
	getHandler func(*archive.CollectionMetadata) (archive.DemuxOut, error),
) error {
	demux := archive.CreateDemux(meta, r.r)
	for _, coll := range meta {
		ns := coll.Database + "." + coll.Collection
		out, err := getHandler(coll)
		if err != nil {
			return err
		}
		if out == nil {
			out = &archive.MutedCollection{}
		}
		demux.Open(ns, out)
	}
	return demux.Run()
}

type preludeParserConsumer struct {
	header      archive.Header
	collections []*archive.CollectionMetadata
}

func (c *preludeParserConsumer) HeaderBSON(b []byte) error {
	return bson.Unmarshal(b, &c.header)
}

func (c *preludeParserConsumer) BodyBSON(b []byte) error {
	var meta archive.CollectionMetadata
	if err := bson.Unmarshal(b, &meta); err != nil {
		return err
	}
	c.collections = append(c.collections, &meta)
	return nil
}

func (c *preludeParserConsumer) End() error {
	return nil
}

type demuxOut struct {
	wc   io.WriteCloser
	hash hash.Hash64
	out  io.Writer
}

func newDemuxOut(wc io.WriteCloser) *demuxOut {
	d := &demuxOut{
		wc:   wc,
		hash: crc64.New(crc64.MakeTable(crc64.ECMA)),
	}
	d.out = io.MultiWriter(d.hash, d.wc)
	return d
}

func (d *demuxOut) Write(in []byte) (int, error) {
	return d.out.Write(in)
}

func (d *demuxOut) Close() error {
	return d.wc.Close()
}

func (d *demuxOut) Sum64() (uint64, bool) {
	return d.hash.Sum64(), true
}
