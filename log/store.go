package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian  //使用大端来编码数据长度，因为长度信息需要进行网络传输 
)

const (
	lenWidth = 8 //8字节用于存储数据长度
)

type store struct {
	*os.File  //对应二进制文件
	mu sync.Mutex  //由于可能同时产生多个读写请求，因此需要加锁
	buf *bufio.Writer //读写二进制数据的接口
	size uint64  //整个文件的大小
}

func newStore(f *os.File) (*store, error) { //传入一个文件句柄来创建store对象
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store {
		File : f,
		size: size,
		buf : bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	//增加一条记录,n表示记录的下标，pos表示记录在二进制文件中的偏移
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size 
	//在写入数据前先用8字节写入数据的长度
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err 
	}
    //然后再写入数据
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err 
	}

	//增加一条记录后，store的大小也要相应改变
	w += lenWidth 
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	//从偏移为pos处读取记录信息
	s.mu.Lock()
	defer s.mu.Unlock()
	//现将缓冲区的数据全部写入到文件
	if err := s.buf.Flush(); err != nil {
		return nil, err 
	}
	//获取记录的长度
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil , err 
	}
    //读取记录的二进制数据
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos + lenWidth)); err != nil {
		return nil, err 
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
    //从二进制文件偏移为off开始将数据读入缓冲区
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err 
	}

	return s.File.Close()
}

