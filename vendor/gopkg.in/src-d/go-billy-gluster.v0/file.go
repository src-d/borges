package gluster

import (
	"fmt"
	"io"
	"os"

	"github.com/gluster/gogfapi/gfapi"
	billy "gopkg.in/src-d/go-billy.v4"
)

var (
	_ billy.File = new(File)
)

type mode int

const (
	// ErrReadOnly returned when file is read only.
	ErrReadOnly = "cannot write in %s, the file is read only"
	// ErrWriteOnly returned when file is write only.
	ErrWriteOnly = "cannot read from %s, the file is write only"

	read  mode = 0
	write mode = 1

	maskRW = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
)

// File holds a gluster file descriptor.
type File struct {
	path  string
	g     *gfapi.File
	flags int
}

// NewFile creates a new wrapped gluster file descriptor.
func NewFile(name string, g *gfapi.File, flags int) *File {
	return &File{path: name, g: g, flags: flags}
}

// Name implements billy.File interface.
func (f *File) Name() string {
	return f.path
}

// Write implements billy.File interface.
func (f *File) Write(p []byte) (int, error) {
	err := f.checkFlags(write)
	if err != nil {
		return 0, err
	}

	return f.g.Write(p)
}

// Read implements billy.File interface.
func (f *File) Read(p []byte) (int, error) {
	err := f.checkFlags(read)
	if err != nil {
		return 0, err
	}

	n, err := f.g.Read(p)
	// on error n is negative
	if n < 0 {
		n = 0
	}
	// it does not tell when the file ended, if we could not read the whole
	// buffer treat it as EOF
	if err == nil && n < len(p) {
		err = io.EOF
	}

	return n, err
}

// ReadAt implements billy.File interface.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	err := f.checkFlags(read)
	if err != nil {
		return 0, err
	}

	offset, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}

	n, err := f.g.ReadAt(p, off)

	_, e := f.Seek(offset, os.SEEK_SET)
	if e != nil {
		if err == nil {
			err = e
		}
		return 0, err
	}

	// fix negative read bytes number and add EOF, same as Read
	if n < 0 {
		n = 0
	}
	if err == nil && n < len(p) {
		err = io.EOF
	}

	return n, err
}

// Seek implements billy.File interface.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.g.Seek(offset, whence)
}

// Close implements billy.File interface.
func (f *File) Close() error {
	return f.g.Close()
}

// Lock implements billy.File interface. It is a no-op as it is not
// supported by gluster library.
func (f *File) Lock() error {
	return nil
}

// Unlock implements billy.File interface. It is a no-op as it is not
// supported by gluster library.
func (f *File) Unlock() error {
	return nil
}

// Truncate implements billy.File interface.
func (f *File) Truncate(size int64) error {
	return f.g.Truncate(size)
}

func (f *File) checkFlags(expected mode) error {
	switch expected {
	case read:
		if f.flags&maskRW == os.O_WRONLY {
			return fmt.Errorf(ErrWriteOnly, f.path)
		}
	case write:
		if f.flags&maskRW == os.O_RDONLY {
			return fmt.Errorf(ErrReadOnly, f.path)
		}
	default:
		panic("unknown mode")
	}

	return nil
}
