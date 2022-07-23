package heaptuple

import (
	"fmt"
	"unsafe"
)

const (
	MAXALIGN = 8
)

type TupleHeader struct {
	Xmin      uint32
	Xmax      uint32
	Cid       uint32
	Ctid      [6]byte
	Infomask2 uint16
	Infomask  uint16
	Hoff      uint8
	NullBits  []byte
}

func (th TupleHeader) HasNullBits() bool {
	return th.Infomask&0x0001 != 0
}

func (th TupleHeader) AttrCnt() uint16 {
	if len(th.NullBits) > 0 {
		return uint16(len(th.NullBits))
	}
	return th.Infomask2 & 0x07FF
}

type Tuple struct {
	Header          TupleHeader
	Data            map[string]string
	ExtraToastField map[string]EXTERNAL
}

func ParseTupleHeader(bins []byte) TupleHeader {
	ret := **(**TupleHeader)(unsafe.Pointer(&bins))
	ret.NullBits = nil
	return ret
}

func ParseTupleHeader2(th *TupleHeader, bins []byte) {
	hasNulls := th.Infomask&0x0001 != 0
	if !hasNulls {
		return
	}
	attrCnt := th.Infomask2 & 0x07FF
	th.NullBits = make([]byte, attrCnt)
	for i := uint16(0); i < attrCnt; i++ {
		byteIdx := i / 8
		bitIdx := i % 8
		th.NullBits[i] = (bins[byteIdx] >> bitIdx) & 0x01
	}
}

func ParseTupleData(alignments []AttrAlign, th *TupleHeader, bins []byte) (map[string]string, map[string]EXTERNAL, error) {
	parseKV := func(idx, offset int) (k string, v string, typ EXTERNAL, nextOffset int) {
		getNextOffset := func() int {
			if idx == len(alignments)-1 {
				return -1
			}

			item := alignments[idx]
			nextItem := alignments[idx+1]
			padding := map[string]int{
				"c": 1,
				"s": 2,
				"i": 4,
				"d": 8,
			}
			nextPadding, ok := padding[nextItem.TypAlign]
			if !ok {
				panic("unknown alignment rules")
			}
			nextOffset := offset + item.TypLen
			for nextOffset%nextPadding != 0 {
				nextOffset++
			}
			return nextOffset
		}
		item := alignments[idx]
		switch item.TypName {
		case "oid":
			var (
				v     uint32
				bytes = bins[offset : offset+item.TypLen]
			)
			v = **(**uint32)(unsafe.Pointer(&bytes))
			return item.AttName, fmt.Sprintf("%d", v), VARTAG_UNUSED, getNextOffset()
		case "int4":
			var (
				v     int32
				bytes = bins[offset : offset+item.TypLen]
			)
			v = **(**int32)(unsafe.Pointer(&bytes))
			return item.AttName, fmt.Sprintf("%d", v), VARTAG_UNUSED, getNextOffset()
		case "bytea", "text":
			text := ParseVarlena(bins[offset:])
			alignments[idx].TypLen = text.GetLength()
			return item.AttName, string(text.GetData()), text.GetType(), getNextOffset()
		}
		panic("does not support")
	}

	var (
		kv     = make(map[string]string)
		extra  = make(map[string]EXTERNAL)
		k, v   string
		typ    EXTERNAL
		offset int
	)
	for i := 0; i < int(th.AttrCnt()); i++ {
		if th.HasNullBits() && th.NullBits[i] == 0 {
			kv[alignments[i].AttName] = "NULL"
			extra[alignments[i].AttName] = VARTAG_UNUSED
			continue
		}
		k, v, typ, offset = parseKV(i, offset)
		kv[k] = v
		extra[k] = typ
	}
	return kv, extra, nil
}

type PageHeader struct {
	Lsn             [8]byte
	Checksum        uint16
	Flags           uint16
	Lower           uint16
	Upper           uint16
	Special         uint16
	PagesizeVersion uint16
	PruneXid        [4]byte
}

type SlotID struct {
	// 15bits:  offset to tuple (from start of page)
	// 2bits: ignore
	// 15bits:  byte length of tuple
	content uint32
}

func (s SlotID) GetTupleOffset() uint16 {
	return uint16(s.content & 0x7FFF)
}

func (s SlotID) GetTupleLength() uint16 {
	return uint16((s.content >> 17) & 0x7FFF)
}

func (s SlotID) GetTupleSize() uint16 {
	length := uint16((s.content >> 17) & 0x7FFF)
	if length%MAXALIGN == 0 {
		return length
	}
	return length + 8 - (length % 8)
}

type Page struct {
	Header PageHeader
	Slots  []SlotID
	Tuples []Tuple
}

func ReadPage(bytes []byte, alignments []AttrAlign) (page Page, err error) {
	var ret Page
	f := bytes
	headerBytes := f[0:24]
	f = f[24:]
	header := **(**PageHeader)(unsafe.Pointer(&headerBytes))
	ret.Header = header

	slotCnt := (ret.Header.Lower - 24) / 4
	ret.Slots = make([]SlotID, slotCnt)
	ret.Tuples = make([]Tuple, slotCnt)
	for idx := range ret.Slots {
		slotBytes := f[:4]
		f = f[4:]
		slot := **(**SlotID)(unsafe.Pointer(&slotBytes))
		ret.Slots[idx] = slot

		tOffset := slot.GetTupleOffset()
		tHeader := ParseTupleHeader(bytes[tOffset : tOffset+23])
		if tHeader.HasNullBits() {
			ParseTupleHeader2(&tHeader, bytes[tOffset+23:tOffset+uint16(tHeader.Hoff)])
		}
		ret.Tuples[idx] = Tuple{Header: tHeader}
		tData, tExtra, err := ParseTupleData(alignments, &tHeader, bytes[tOffset+uint16(tHeader.Hoff):])
		if err != nil {
			return Page{}, err
		}
		ret.Tuples[idx].Data = tData
		ret.Tuples[idx].ExtraToastField = tExtra
	}
	return ret, nil
}
