package log 
import (
	"io/ioutil"
	"os"
	"testing"
	"github.com/stretchr/testify/require"
)

var (
	write = []byte("this is a record")
	//一条记录的数据长度，4个字节表示内容长度，len(write)表示就内容长度
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	//先创建用于存储数据的二进制文件
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f) 
	require.NoError(t, err)
    //测试插入记录
	testAppend(t, s)
    //测试读取一条记录
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	//应该能够正常的插入若干条记录
	for i := uint64(1); i < 4; i ++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos + n, width * i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64 
	//应该能正常的读取插入的记录
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width 
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		//先读取8个字节得到
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		//读取的数据长度要等于缓冲区的长度
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		//读取日志的二进制数据
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		//读取到的内容要跟写入的内容一致
		require.Equal(t, write, b)
		//读取的数据长度要跟开头8字节所表示的长度一致
        require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	
	s, err := newStore(f)
	require.NoError(t, err)
    _, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)
    //检验文件关闭后数据必须写入
	err = s.Close()
	require.NoError(t, err)
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	//由于文件关闭时缓存的数据必须写入磁盘因此文件关闭后的大小要大于关闭前大小
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	//创建用于存储二进制数据的文件
	f, err := os.OpenFile(name, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644,)
	if err != nil {
		return nil, 0, err 
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err 
	}

	return f, fi.Size(), nil 
}

