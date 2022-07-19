package heaptuple

import "unsafe"

type Varlena interface {
	GetLength() int
	GetDataLength() int
	GetData() []byte
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

type VarAttrib4B struct {
	Header  uint32
	RawSize uint32
	Bytes   []byte
}

func (v VarAttrib4B) GetDataLength() int {
	return v.GetLength() - 4
}

func (v VarAttrib4B) GetLength() int {
	return int(v.Header >> 2)
}

func (v VarAttrib4B) GetData() []byte {
	return v.Bytes
}

func (v VarAttrib4B) IsCompressed() bool {
	return v.Header&0x10 == 0x10
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
	case header&0x01 == 0x00:
		tmp := VarAttrib4B{
			Header: **(**uint32)(unsafe.Pointer(&bins)),
		}
		cnt := tmp.GetDataLength()
		bins = bins[4:]
		for i := 0; i < cnt; i++ {
			tmp.Bytes = append(tmp.Bytes, bins[i])
		}
		return tmp
	}
	return nil
}
