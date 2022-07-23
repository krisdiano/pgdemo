package heaptuple

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"unsafe"

	"github.com/jackc/pgx/v5"
)

type AttrAlign struct {
	AttName  string
	TypName  string
	TypAlign string
	TypLen   int
}

type Table struct {
	selfPath       string
	toastPath      string
	selfAttrAlign  []AttrAlign
	toastAttrAlign []AttrAlign
	selfFiles      []HeapFile
	toastFiles     []HeapFile
}

func NewTable(table string) (t Table, err error) {
	ctx := context.Background()
	url := "postgres://localhost:8432/litianxiang"
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)

	}
	defer conn.Close(context.Background())

	var pgdata string
	row := conn.QueryRow(ctx, "show data_directory;")
	err = row.Scan(&pgdata)
	if err != nil {
		return
	}

	selfPath, toastPath, err := getSelfAndToastAbsPath(ctx, conn, table, pgdata)
	if err != nil {
		return
	}
	selfAttrAlign, err := getAlign(ctx, conn, table)
	if err != nil {
		return
	}
	toastAttrAlign, err := getAlign(ctx, conn, fmt.Sprintf("pg_toast_%s", filepath.Base(selfPath)))
	if err != nil {
		return
	}
	selfFile, err := ReadHeapFile(selfPath, 1024*8, selfAttrAlign)
	if err != nil {
		return
	}
	toastFile, err := ReadHeapFile(toastPath, 1024*8, toastAttrAlign)
	if err != nil {
		return
	}

	return Table{
		selfPath:       selfPath,
		toastPath:      toastPath,
		selfAttrAlign:  selfAttrAlign,
		toastAttrAlign: toastAttrAlign,
		selfFiles:      []HeapFile{selfFile},
		toastFiles:     []HeapFile{toastFile},
	}, nil
}

func getSelfAndToastAbsPath(ctx context.Context, conn *pgx.Conn, table, pgdata string) (string, string, error) {
	var fpath string
	row := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT pg_relation_filepath('%s')", table))
	err := row.Scan(&fpath)
	if err != nil {
		return "", "", err
	}

	dir, filename := filepath.Split(fpath)
	row = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT pg_relation_filepath('%s')", fmt.Sprintf("pg_toast.pg_toast_%s", filename)))
	err = row.Scan(&fpath)
	if err != nil {
		return "", "", err
	}
	toastFilename := fmt.Sprintf("%s", filepath.Base(fpath))
	fAbsPath := filepath.Join(pgdata, dir, filename)
	tAbsPath := filepath.Join(pgdata, dir, toastFilename)
	return fAbsPath, tAbsPath, nil
}

func getAlign(ctx context.Context, conn *pgx.Conn, table string) ([]AttrAlign, error) {
	var alignSQL = `
SELECT a.attname, t.typname, t.typalign::text, t.typlen
  FROM pg_class c
  JOIN pg_attribute a ON (a.attrelid = c.oid)
  JOIN pg_type t ON (t.oid = a.atttypid)
 WHERE c.relname = '%s'
   AND a.attnum >= 0
 ORDER BY a.attnum;
`
	rows, err := conn.Query(ctx, fmt.Sprintf(alignSQL, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alignments []AttrAlign
	for rows.Next() {
		var item AttrAlign
		err = rows.Scan(&item.AttName, &item.TypName, &item.TypAlign, &item.TypLen)
		if err != nil {
			return nil, err
		}
		alignments = append(alignments, item)
	}
	return alignments, nil
}

func (t Table) GetTuples() []map[string]string {
	copyMap := func(m map[string]string) map[string]string {
		ret := make(map[string]string)
		for k, v := range m {
			ret[k] = v
		}
		return ret
	}
	var ret []map[string]string
	for _, hp := range t.selfFiles {
		for _, p := range hp.Pages {
			for _, tp := range p.Tuples {
				kv := copyMap(tp.Data)
				for column, toastTyp := range tp.ExtraToastField {
					switch toastTyp {
					case VARTAG_UNUSED:
						continue
					case VARTAG_ONDISK:
						bytes := t.onDiskTransfer(column, []byte(kv[column]))
						kv[column] = t.fieldTransfer(column, bytes)
					default:
						panic(fmt.Errorf("only support on disk, received %d", toastTyp))
					}
				}
				ret = append(ret, kv)
			}
		}
	}
	return ret
}

func (t Table) onDiskTransfer(column string, bytes []byte) []byte {
	toastOnDisk := **(**ExternalOnDisk)(unsafe.Pointer(&bytes))

	type sortItem struct {
		seq     int
		content string
	}
	var buffer []sortItem
	for _, hp := range t.toastFiles {
		for _, p := range hp.Pages {
			for _, tp := range p.Tuples {
				if tp.Data["chunk_id"] != fmt.Sprintf("%d", toastOnDisk.ValueOID) {
					continue
				}
				seq, err := strconv.Atoi(tp.Data["chunk_seq"])
				if err != nil {
					panic("parse chunk_seq failed")
				}
				buffer = append(buffer, sortItem{seq: seq, content: tp.Data["chunk_data"]})
			}
			sort.Slice(buffer, func(i, j int) bool { return buffer[i].seq < buffer[j].seq })
		}
	}
	var ret []byte
	for _, item := range buffer {
		ret = append(ret, []byte(item.content)...)
	}
	return ret
}

func (t Table) fieldTransfer(column string, bytes []byte) string {
	for _, item := range t.selfAttrAlign {
		if item.AttName != column {
			continue
		}
		switch item.TypName {
		case "text":
			return string(bytes)
		}
	}
	panic("not support")
}
