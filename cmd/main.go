package main

import (
	"fmt"

	wal "github.com/srivastavcodes/write-ahead-log"
)

func main() {
	waLog, _ := wal.Open(wal.DefaultOptions)
	defer func() {
		_ = waLog.Delete()
	}()

	// writing data to log
	pos1, _ := waLog.Write([]byte("example data one"))
	pos2, _ := waLog.Write([]byte("example data two"))

	// reading data sequentially
	data1, _ := waLog.Read(pos1)
	fmt.Printf("data1: %s\n", data1)

	data2, _ := waLog.Read(pos2)
	fmt.Printf("data1: %s\n", data2)

	fmt.Println()

	_, _ = waLog.Write([]byte("example data 3"))
	_, _ = waLog.Write([]byte("example data 4"))
	_, _ = waLog.Write([]byte("example data 5"))
	_, _ = waLog.Write([]byte("example data 6"))

	reader := waLog.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		fmt.Printf("pos: %v, data: %s\n", pos, data)
	}
}
