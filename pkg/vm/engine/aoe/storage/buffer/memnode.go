package buf

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

func RawMemoryNodeConstructor(vf common.IVFile, useCompress bool, freeFunc MemoryFreeFunc) IMemoryNode {
	return NewRawMemoryNode(vf, useCompress, freeFunc)
}

type RawMemoryNode struct {
	Data        []byte
	FreeFunc    MemoryFreeFunc
	UseCompress bool
	File        common.IVFile
}

func NewRawMemoryNode(vf common.IVFile, useCompress bool, freeFunc MemoryFreeFunc) IMemoryNode {
	var capacity int64
	if useCompress {
		capacity = vf.Stat().Size()
	} else {
		capacity = vf.Stat().OriginSize()
	}
	node := &RawMemoryNode{
		FreeFunc:    freeFunc,
		UseCompress: useCompress,
		File:        vf,
		Data:        make([]byte, capacity),
	}
	return node
}

func (mn *RawMemoryNode) GetMemoryCapacity() uint64 {
	return uint64(cap(mn.Data))
}

func (mn *RawMemoryNode) GetMemorySize() uint64 {
	return uint64(len(mn.Data))
}

func (mn *RawMemoryNode) FreeMemory() {
	mn.FreeFunc(mn)
}

func (mn *RawMemoryNode) Reset() {
	mn.Data = mn.Data[:0]
}

func (mn *RawMemoryNode) WriteTo(w io.Writer) (n int64, err error) {
	nw, err := w.Write(mn.Data)
	return int64(nw), err
}

func (mn *RawMemoryNode) ReadFrom(r io.Reader) (n int64, err error) {
	if len(mn.Data) != cap(mn.Data) {
		panic("logic error")
	}
	nr, err := r.Read(mn.Data)
	return int64(nr), err
}

func (mn *RawMemoryNode) Marshall() (buf []byte, err error) {
	buf = append(mn.Data[0:0:0], mn.Data...)
	return buf, err
}

func (mn *RawMemoryNode) Unmarshall(buf []byte) error {
	length := cap(mn.Data)
	if length > len(buf) {
		length = len(buf)
	}
	copy(mn.Data, buf[0:length])
	return nil
}
