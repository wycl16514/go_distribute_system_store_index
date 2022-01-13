package log 

import (
	"io"
    "os"
	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	//4字节用来表示记录的下标，8字节表示记录在二进制文件中的偏移
	entWidth = offWidth + posWidth 
)

type index struct {
	file *os.File  //存储文件
	mmap gommap.MMap  //文件内容在内存中的映射
	size uint64 
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index {
		file : f, 
	}

	fi , err := os.Stat(f.Name())
	if err != nil {
		return nil , err
	}

	idx.size = uint64(fi.Size())
	//现将文件扩大到指定大小以便接下来使用内存映射
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes),); err != nil {
		return nil, err 
	}
    //启用内存映射加快文件的读写速度
	if idx.mmap , err = gommap.Map(idx.file.Fd(), gommap.PROT_READ | gommap.PROT_WRITE, gommap.MAP_SHARED,); err != nil {
		return nil, err 
	}

	return idx, nil
}

func (i *index) Close() error  {
	//关闭文件时先将内存中的数据写入文件,这里要在linux系统运行，在windows运行会出错
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

    //将文件缓存的数据写入磁盘
	if err := i.file.Sync(); err != nil {
		return err 
	}

	//将文件的大小设置为实际写入数据的大小
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err 
	}

	return i.file.Close() 
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF 
	}
    //in==-1表示读取最后一条记录
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth 
	if i.size < pos + entWidth {
		return 0, 0, io.EOF 
	}

	out = enc.Uint32(i.mmap[pos : pos + offWidth]) //记录的下标
	pos = enc.Uint64(i.mmap[pos + offWidth : pos + entWidth])  //记录在二进制文件中的偏移

	return out, pos, nil 
}

func (i *index) Write(off uint32, pos uint64) error {
	//新增一条记录的索引
	if uint64(len(i.mmap)) < i.size + entWidth {
		return io.EOF 
	}

	enc.PutUint32(i.mmap[i.size : i.size + offWidth], off)  //新增记录的下标
	enc.PutUint64(i.mmap[i.size + offWidth : i.size + entWidth], pos) //新增记录在二进制文件中的偏移
	i.size += uint64(entWidth)

	return nil 
}

func (i *index) Name() string {
	return i.file.Name()
}