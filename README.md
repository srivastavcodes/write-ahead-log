# Write-Ahead-Log

WAL is a small, append-only write-ahead log written in Go. It’s built to be simple,
predictable, and easy to reason about.

The core idea behind it is — append records to disk safely, split large records
across fixed-size blocks, verify integrity with checksums, and read them back
sequentially for recovery or replay.

## Features

- Append-only log with segment-based storage.
    - Uses fixed-size blocks internally with chunked records.
- Supports large records spanning multiple blocks.
- `crc32` checksums for corruption detection.
- Sequential reads with precise resume positions.
- Startup traversal with block caching for recovery.
- Supports concurrent read/write, has thread-safe functions.

## Design Overview

![wal-logo.png](https://s2.loli.net/2025/01/12/SF9vThRkAObm4WD.png)

## Format

**Format of a single segment file:**

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |      r1     |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       |<---- BlockSize ----->|<---- BlockSize ----->|

  rn = variable size records
  P = Padding
  BlockSize = 32KB
```

**Format of a single record:**

```
+----------+-------------+-----------+--- ... ---+
| CRC (4B) | Length (2B) | Type (1B) |  Payload  |
+----------+-------------+-----------+--- ... ---+

CRC = 32-bit hash computed over the payload using CRC
Length = Length of the payload data
Type = Type of record
       (FullType, FirstType, MiddleType, LastType)
       The type is used to group a bunch of records together to represent
       blocks that are larger than BlockSize
Payload = Byte stream as long as specified by the payload size
```

## Getting Started

```go
func main() {
    waLog, _ := wal.Open(wal.DefaultOptions)
    defer func () {
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
```

## How it works (on a high level)

- Data is written to the active segment sequentially.
- Each segment is divided into fixed-size blocks.
- Records are split into chunks when needed.
- Each chunk is checksummed independently.
- Reads walk blocks and chunks to reconstruct records.
- Startup traversal reuses cached blocks when possible.

## Status

This is a learning project. As a result, it focuses on correctness and clarity
over production features, and the API may change as the implementation evolves.
