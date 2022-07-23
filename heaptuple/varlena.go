package heaptuple

import (
	"unsafe"
)

type Varlena interface {
	GetLength() int
	GetDataLength() int
	GetData() []byte
	GetType() EXTERNAL
}

type EXTERNAL = uint8

const (
	VARTAG_UNUSED      EXTERNAL = 0
	VARTAG_INDIRECT    EXTERNAL = 1
	VARTAG_EXPANDED_RO EXTERNAL = 2
	VARTAG_EXPANDED_RW EXTERNAL = 3
	VARTAG_ONDISK      EXTERNAL = 18
)

type ExternalOnDisk struct {
	RawSize  int32
	ExtSize  int32
	ValueOID uint32
	ToastOID uint32
}

// Only for little endian
type VarAttrib1B struct {
	Header uint8
	Bytes  []byte
}

func (v VarAttrib1B) GetDataLength() int {
	return v.GetLength() - 1
}

func (v VarAttrib1B) GetLength() int {
	return int(v.Header >> 1)
}

func (v VarAttrib1B) GetData() []byte {
	return v.Bytes
}

func (v VarAttrib1B) GetType() EXTERNAL {
	return VARTAG_UNUSED
}

type VarAttrib1BE struct {
	Header uint8
	Tag    uint8
	Bytes  []byte
}

func (v VarAttrib1BE) GetDataLength() int {
	switch v.Tag {
	case VARTAG_INDIRECT:
		return MAXALIGN
	case VARTAG_EXPANDED_RO:
		return MAXALIGN
	case VARTAG_EXPANDED_RW:
		return MAXALIGN
	case VARTAG_ONDISK:
		return 16
	default:
		panic("invalid tag")
	}
}

func (v VarAttrib1BE) GetLength() int {
	return v.GetDataLength() + 2
}

func (v VarAttrib1BE) GetData() []byte {
	return v.Bytes
}

func (v VarAttrib1BE) GetType() EXTERNAL {
	return v.Tag
}

type VarAttrib4B struct {
	Header  uint32
	RawSize uint32
	Bytes   []byte
}

func (v VarAttrib4B) GetDataLength() int {
	if !v.IsCompressed() {
		return v.GetLength() - 4
	}
	return v.GetLength() - 8
}

func (v VarAttrib4B) GetLength() int {
	return int(v.Header >> 2)
}

func (v VarAttrib4B) GetData() []byte {
	if !v.IsCompressed() {
		return v.Bytes
	}
	ret := make([]byte, int(v.RawSize))
	err := Decompress(v.Bytes, ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func (v VarAttrib4B) IsCompressed() bool {
	return v.Header&0x03 == 0x02
}

func (v VarAttrib4B) GetType() EXTERNAL {
	return VARTAG_UNUSED
}

func ParseVarlena(bins []byte) Varlena {
	header := bins[0]
	switch {
	case header&0x01 == 0x01 && header != 0x01:
		tmp := VarAttrib1B{
			Header: header,
		}
		cnt := tmp.GetDataLength()
		bins = bins[1:]
		for i := 0; i < cnt; i++ {
			tmp.Bytes = append(tmp.Bytes, bins[i])
		}
		return tmp
	case header == 0x01:
		tmp := VarAttrib1BE{
			Header: header,
			Tag:    bins[1],
		}
		cnt := tmp.GetDataLength()
		bins = bins[2:]
		for i := 0; i < cnt; i++ {
			tmp.Bytes = append(tmp.Bytes, bins[i])
		}
		return tmp
	case header&0x03 == 0x00:
		tmp := VarAttrib4B{
			Header: **(**uint32)(unsafe.Pointer(&bins)),
		}
		cnt := tmp.GetDataLength()
		bins = bins[4:]
		for i := 0; i < cnt; i++ {
			tmp.Bytes = append(tmp.Bytes, bins[i])
		}
		return tmp
	case header&0x03 == 0x02:
		tmp := VarAttrib4B{
			Header: **(**uint32)(unsafe.Pointer(&bins)),
		}
		rawSize := bins[4:8]
		tmp.RawSize = **(**uint32)(unsafe.Pointer(&rawSize))

		cnt := tmp.GetDataLength()
		bins = bins[8:]
		for i := 0; i < cnt; i++ {
			tmp.Bytes = append(tmp.Bytes, bins[i])
		}
		return tmp
	}
	panic("no support sub varlena type")
}
