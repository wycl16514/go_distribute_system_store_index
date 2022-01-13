package log 

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024 //这里表示存储记录二进制数据对应文件的最大长度
	idx, err := newIndex(f , c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32 
		Pos uint64 
	} {
		//这里虚构两条记录对应的索引
		{Off: 0, Pos: 2},
		{Off: 1, Pos: 10},
	}

	//测试读取出来的索引内容要与写入的内容一致
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	//读取的数据超出范围时要返回错误，例如当前只写入了3条记录对应的索引，但却要读取第4条记录索引时就要返回错误io.EOF
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	err = idx.Close()
	
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx , err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)

	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)

	
}