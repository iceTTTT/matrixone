package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// log "github.com/sirupsen/logrus"
)

type EmbedIndexFile struct {
	SegmentFile base.ISegmentFile
	Meta        *base.IndexMeta
	Info        *fileStat
}

type EmbedBlockIndexFile struct {
	EmbedIndexFile
	ID common.ID
}

func newEmbedIndexFile(host base.ISegmentFile, meta *base.IndexMeta) common.IVFile {
	f := &EmbedIndexFile{
		SegmentFile: host,
		Meta:        meta,
		Info: &fileStat{
			size: int64(meta.Ptr.Len),
		},
	}
	return f
}

func newEmbedBlockIndexFile(id *common.ID, host base.ISegmentFile, meta *base.IndexMeta) common.IVFile {
	f := &EmbedBlockIndexFile{
		EmbedIndexFile: EmbedIndexFile{
			SegmentFile: host,
			Meta:        meta,
			Info: &fileStat{
				size:  int64(meta.Ptr.Len),
				osize: int64(meta.Ptr.Len),
			},
		},
		ID: *id,
	}
	f.Ref()
	return f
}

func (f *EmbedIndexFile) Stat() common.FileInfo {
	return f.Info
}

func (f *EmbedIndexFile) Ref() {
	f.SegmentFile.Ref()
}

func (f *EmbedIndexFile) Unref() {
	f.SegmentFile.Ref()
}

func (cpf *EmbedIndexFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (f *EmbedIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(f.Meta.Ptr.Len) {
		panic("logic error")
	}
	f.SegmentFile.ReadPoint(f.Meta.Ptr, buf)
	return len(buf), nil
}

func (bf *EmbedBlockIndexFile) Stat() common.FileInfo {
	return bf.Info
}
func (bf *EmbedBlockIndexFile) Ref() {
	bf.SegmentFile.RefBlock(bf.ID)
}

func (bf *EmbedBlockIndexFile) Unref() {
	bf.SegmentFile.UnrefBlock(bf.ID)
}

func (bf *EmbedBlockIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(bf.Meta.Ptr.Len) {
		panic("logic error")
	}
	bf.SegmentFile.ReadBlockPoint(bf.ID, bf.Meta.Ptr, buf)
	return len(buf), nil
}
