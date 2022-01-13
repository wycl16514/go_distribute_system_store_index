上一节我们实现了日志微服务，它以http服务器的模式运行，客户端通过json方式将日志数据post过来，然后通过http get的方式读取日志。当时我们的实现是将所有日志信息添加到数组末尾，这意味着所有日志信息都会保存在内存中。但分布式系统的日志数量将非常巨大，例如推特一天的日志数量就达到一万亿，国内微博，微信，淘宝等超大规模系统的日志数量估计也是这个等级。假设我们使用一百台服务器运行日志微服务，那么一台将处理10亿条日志，再假设一条日志为64字节，那么如果直接将日志存放在内存就需要消耗64G，再考虑到很多日志存储后很可能再读取，而且一台服务器还需要提供其他程序运行，因此直接将日志存储在内存将是一种巨大的损耗。

因此我们需要一种有效的文件系统来存储这么多的日志信息，而且存储机制还需要支持快速查询，当然我们可以采用mysql等数据库存储日志，但这类数据库在查询速度上难以足够快，因此我们有必要自行设计满足需求的存储系统，改系统要满足能快速的在海量数据中迅速查询所需要的记录。

处理海量数据或高并发需求的基本思路其实就是分而治之。想想我们全国14亿人，你发快递的话，顺丰这些服务商如何快速将包裹准确的发送给接收人呢。思路其实很简单，首先你需要确定接收人所在省份，然后确定市区，接着确定乡镇，再接着确定街道小区，最后确定楼房单元，通过这种不断“分区”进而快速缩小查找范围的方式就能快速的定位目标。对于10亿条日志，我们同样采用“分区”思路，将他们分成100份，每份一千万条记录，第一份记录编号为0到9999999的日志，第二份记录10000000到19999999的日志，以此类推。假设当我们要查询编号为一千一百万的日志时，我们到第二份里面查找即可，如果想要加快速度，那么每一份还可以继续往下拆分。

首先我们看如何存储日志的二进制数据。日志其实是一串二进制数据，因此我们采用最简单的存储机制如下：
长度，数据内容|长度，数据内容|。。。
也就是存储日志二进制数据时，我们先存储其长度，然后写入二进制数据，然后存储第二条日志的长度，跟着第二条日志的二进制数据，其中”长度“我们用8个字节来表示，以此类推。但这种存储方式存在一个问题，那就是查询会很慢，假设我们要读取第n条日志内容，我们必须从头开始，先获得第一天数据的长度，然后越过给定长度，接着读取第二条数据长度，然后继续越过第二条数据长度，依次类推，因此读取第n条记录的时间复杂度就是O(n)。

导致查询速度慢的原因就在于每条数据长度不一，读取第n条记录就需要依次解析前面n-1条记录的长度，为了加快速度我们需要设立一个索引文件，该文件直接记录第n条记录在二进制文件中的偏移。假设在二进制文件中存储了三条记录，第1条记录数据长度为4字节，第二2条记录长度为8字节，第3条记录长度为12字节，那么索引文件的格式为：
0，0|1，12|2，28|
我们看看索引文件的逻辑，“0，0“表示第0条数据从二进制文件的偏移为0处开始读取，”1，8“，表示第1条日志的数据从二进制文件的偏移为8处开始，”2，20“表示第2条日志的数据从二进制文件偏移为20字节处开始读取。

因为第0条数据它的长度放置在二进制文件开头，接着就是4字节的数据，因此它在二进制文件中的偏移就是0，由于8字节用来表示长度，同时数据又占据了4字节，因此偏移12字节后对应的8字节数据就是第二条数据的长度，由于第二条数据的长度为8，因此继续偏移8字节就得到第3条数据长度的起始位置，因此就是偏移28个字节后就是第3条数据对应长度的8个字节，因此索引文件中记录了”2，28“,也就是下标为2的日志，在二进制文件中的起始偏移为28。

索引日志中用4个字节表示记录下标，用8个字节表示数据偏移，于是每12个字节就能表达出一条记录在二进制文件中的偏移，因此我们要想快速定位第n条记录在二进制文件中的起始位置，我们直接从索引文件中偏移为(n-1)*12 + 8处读取8个字节的数据就得到了第n条记录在二进制文件中的起始位置。

我们用store来表示存储数据的二进制文件，".store"来对应存储数据的二进制文件后缀，用inex表示索引，".index"作为索引文件的后缀，由此先看二进制文件的实现，在internal目录下创建一个文件夹名为log，然后创建store.go文件，输入如下代码：
```
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
```
存储记录二进制数据的对象叫store，它对应一个二进制文件，它的读写逻辑跟我们前面描述的一样。接下来我们通过测试将上面实现的逻辑运行起来，在同样目录下创建store_test.go，输入测试用例如下：
```
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


```
接下来看看索引文件的实现，在相同路径下创建index.go文件，输入代码如下：
```
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
```
有了索引文件的实现后，我们测试其逻辑，新建index_test.go，然后输入以下代码:
```
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
```
到现在为止，我们仅仅完成了数据的存储和索引，我们还需要完成的工作有，将海量数据分成多个store及其对应的index，相应的工作我们在下一节进行。

