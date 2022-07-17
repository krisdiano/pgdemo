package heaptuple

// Only for little endian
type Varlena1B struct {
	Header uint8
	Bytes  []byte
}

func (v Varlena1B) GetDataLength() int {
	return v.GetLength() - 1
}

func (v Varlena1B) GetLength() int {
	return int(v.Header >> 1)
}

func ParseVarlena(bins []byte) Varlena1B {
	header := bins[0]
	if header&0x01 != 0x01 || header == 0x01 {
		panic("only support vaelena_1b")
	}

	tmp := Varlena1B{
		Header: header,
	}
	cnt := tmp.GetDataLength()
	bins = bins[1:]
	for i := 0; i < cnt; i++ {
		tmp.Bytes = append(tmp.Bytes, bins[i])
	}
	return tmp
}
