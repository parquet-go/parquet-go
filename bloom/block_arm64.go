//go:build !purego && arm64

package bloom

//go:noescape
func blockInsert(b *Block, x uint32)

//go:noescape
func blockCheck(b *Block, x uint32) bool

func (b *Block) Insert(x uint32) {
	blockInsert(b, x)
}

func (b *Block) Check(x uint32) bool {
	return blockCheck(b, x)
}

func (f SplitBlockFilter) insertBulk(x []uint64) {
	for i := range x {
		f.Insert(x[i])
	}
}
