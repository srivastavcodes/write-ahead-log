package main

import "wal/bufpool"

func main() {
	pool := bufpool.GetFromBuf()
	defer bufpool.PutIntoBuf(pool)
}
