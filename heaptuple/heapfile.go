package heaptuple

import (
	"os"
)

type HeapFile struct {
	Pages []Page
}

func ReadHeapFile(path string, pageSize int, alignments []AttrAlign) (hf HeapFile, err error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return
	}

	size := len(bytes)
	pageOffset := []int{0}
	pageLastIdx := 0
	for pageOffset[pageLastIdx]+pageSize < size {
		pageLastIdx++
		pageOffset = append(pageOffset, pageLastIdx*pageSize)
	}
	pageLastIdx++
	pageOffset = append(pageOffset, size)

	var pages []Page
	for idx := range pageOffset[:pageLastIdx] {
		start, end := pageOffset[idx], pageOffset[idx+1]
		p, err := ReadPage(bytes[start:end], alignments)
		if err != nil {
			return hf, err
		}
		pages = append(pages, p)
	}
	hf.Pages = pages

	return
}
