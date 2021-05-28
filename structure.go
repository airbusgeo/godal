package godal

// Block is a window inside a dataset, starting at pixel X0,Y0 and spanning
// W,H pixels.
type Block struct {
	X0, Y0 int
	W, H   int
	bw, bh int //block size
	sx, sy int //img size
	nx, ny int //num blocks
	i, j   int //cur
}

// Next returns the following block in scanline order. It returns Block{},false
// when there are no more blocks in the scanlines
func (b Block) Next() (Block, bool) {
	nb := b
	nb.i++
	if nb.i >= nb.nx {
		nb.i = 0
		nb.j++
	}
	if nb.j >= nb.ny {
		return Block{}, false
	}
	nb.X0 = nb.i * nb.bw
	nb.Y0 = nb.j * nb.bh
	nb.W, nb.H = actualBlockSize(nb.sx, nb.sy, nb.bw, nb.bh, nb.i, nb.j)

	return nb, true
}

// BlockIterator returns the blocks covering a sizeX,sizeY dataset.
// All sizes must be strictly positive.
func BlockIterator(sizeX, sizeY int, blockSizeX, blockSizeY int) Block {
	bl := Block{
		X0: 0,
		Y0: 0,
		i:  0,
		j:  0,
		bw: blockSizeX,
		bh: blockSizeY,
		sx: sizeX,
		sy: sizeY,
	}
	bl.nx, bl.ny = (sizeX+blockSizeX-1)/blockSizeX,
		(sizeY+blockSizeY-1)/blockSizeY
	bl.W, bl.H = actualBlockSize(sizeX, sizeY, blockSizeX, blockSizeY, 0, 0)
	return bl
}

// BandStructure implements Structure for a Band
type BandStructure struct {
	SizeX, SizeY           int
	BlockSizeX, BlockSizeY int
	DataType               DataType
}

// DatasetStructure implements Structure for a Dataset
type DatasetStructure struct {
	BandStructure
	NBands int
}

// FirstBlock returns the topleft block definition
func (is BandStructure) FirstBlock() Block {
	return BlockIterator(is.SizeX, is.SizeY, is.BlockSizeX, is.BlockSizeY)
}

// BlockCount returns the number of blocks in the x and y dimensions
func (is BandStructure) BlockCount() (int, int) {
	return (is.SizeX + is.BlockSizeX - 1) / is.BlockSizeX,
		(is.SizeY + is.BlockSizeY - 1) / is.BlockSizeY
}

// ActualBlockSize returns the number of pixels in the x and y dimensions
// that actually contain data for the given x,y block
func (is BandStructure) ActualBlockSize(blockX, blockY int) (int, int) {
	return actualBlockSize(is.SizeX, is.SizeY, is.BlockSizeX, is.BlockSizeY, blockX, blockY)
}

func actualBlockSize(sizeX, sizeY int, blockSizeX, blockSizeY int, blockX, blockY int) (int, int) {
	cx, cy := (sizeX+blockSizeX-1)/blockSizeX,
		(sizeY+blockSizeY-1)/blockSizeY
	if blockX < 0 || blockY < 0 || blockX >= cx || blockY >= cy {
		return 0, 0
	}
	retx := blockSizeX
	rety := blockSizeY
	if blockX == cx-1 {
		nXPixelOff := blockX * blockSizeX
		retx = sizeX - nXPixelOff
	}
	if blockY == cy-1 {
		nYPixelOff := blockY * blockSizeY
		rety = sizeY - nYPixelOff
	}
	return retx, rety
}
