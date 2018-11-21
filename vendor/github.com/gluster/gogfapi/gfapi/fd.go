package gfapi

// This file includes lower level operations on fd like the ones in the 'syscall' package

// #cgo pkg-config: glusterfs-api
// #include "glusterfs/api/glfs.h"
// #include <stdlib.h>
// #include <sys/stat.h>
import "C"
import (
	"syscall"
	"unsafe"
)

// Fd is the glusterfs fd type
type Fd struct {
	fd *C.glfs_fd_t
}

var _zero uintptr

// Fchmod changes the mode of the Fd to the given mode
//
// Returns error on failure
func (fd *Fd) Fchmod(mode uint32) error {
	_, err := C.glfs_fchmod(fd.fd, C.mode_t(mode))

	return err
}

// Fstat performs an fstat call on the Fd and saves stat details in the passed stat structure
//
// Returns error on failure
func (fd *Fd) Fstat(stat *syscall.Stat_t) error {

	ret, err := C.glfs_fstat(fd.fd, (*C.struct_stat)(unsafe.Pointer(stat)))
	if int(ret) < 0 {
		return err
	}
	return nil
}

// Fsync performs an fsync on the Fd
//
// Returns error on failure
func (fd *Fd) Fsync() error {
	ret, err := C.glfs_fsync(fd.fd)
	if ret < 0 {
		return err
	}
	return nil
}

// Ftruncate truncates the size of the Fd to the given size
//
// Returns error on failure
func (fd *Fd) Ftruncate(size int64) error {
	_, err := C.glfs_ftruncate(fd.fd, C.off_t(size))

	return err
}

// Pread reads at most len(b) bytes into b from offset off in Fd
//
// Returns number of bytes read on success and error on failure
func (fd *Fd) Pread(b []byte, off int64) (int, error) {
	n, err := C.glfs_pread(fd.fd, unsafe.Pointer(&b[0]), C.size_t(len(b)), C.off_t(off), 0)

	return int(n), err
}

// Pwrite writes len(b) bytes from b into the Fd from offset off
//
// Returns number of bytes written on success and error on failure
func (fd *Fd) Pwrite(b []byte, off int64) (int, error) {
	n, err := C.glfs_pwrite(fd.fd, unsafe.Pointer(&b[0]), C.size_t(len(b)), C.off_t(off), 0)

	return int(n), err
}

// Read reads at most len(b) bytes into b from Fd
//
// Returns number of bytes read on success and error on failure
func (fd *Fd) Read(b []byte) (n int, err error) {
	var p0 unsafe.Pointer

	if len(b) > 0 {
		p0 = unsafe.Pointer(&b[0])
	} else {
		p0 = unsafe.Pointer(&_zero)
	}

	// glfs_read returns a ssize_t. The value of which is the number of bytes written.
	// Unless, ret is -1, an error, implying to check errno. cgo collects errno as the
	// functions error return value.
	ret, e1 := C.glfs_read(fd.fd, p0, C.size_t(len(b)), 0)
	n = int(ret)
	if n < 0 {
		err = e1
	}

	return n, err
}

// Write writes len(b) bytes from b into the Fd
//
// Returns number of bytes written on success and error on failure
func (fd *Fd) Write(b []byte) (n int, err error) {
	var p0 unsafe.Pointer

	if len(b) > 0 {
		p0 = unsafe.Pointer(&b[0])
	} else {
		p0 = unsafe.Pointer(&_zero)
	}

	// glfs_write returns a ssize_t. The value of which is the number of bytes written.
	// Unless, ret is -1, an error, implying to check errno. cgo collects errno as the
	// functions error return value.
	ret, e1 := C.glfs_write(fd.fd, p0, C.size_t(len(b)), 0)
	n = int(ret)
	if n < 0 {
		err = e1
	}

	return n, err
}

func (fd *Fd) lseek(offset int64, whence int) (int64, error) {
	ret, err := C.glfs_lseek(fd.fd, C.off_t(offset), C.int(whence))

	return int64(ret), err
}

func (fd *Fd) Fallocate(mode int, offset int64, len int64) error {
	ret, err := C.glfs_fallocate(fd.fd, C.int(mode),
		C.off_t(offset), C.size_t(len))

	if ret == 0 {
		err = nil
	}
	return err
}

func (fd *Fd) Fgetxattr(attr string, dest []byte) (int64, error) {
	var ret C.ssize_t
	var err error

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	if len(dest) <= 0 {
		ret, err = C.glfs_fgetxattr(fd.fd, cattr, nil, 0)
	} else {
		ret, err = C.glfs_fgetxattr(fd.fd, cattr,
			unsafe.Pointer(&dest[0]), C.size_t(len(dest)))
	}

	if ret >= 0 {
		return int64(ret), nil
	} else {
		return int64(ret), err
	}
}

func (fd *Fd) Fsetxattr(attr string, data []byte, flags int) error {

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	ret, err := C.glfs_fsetxattr(fd.fd, cattr,
		unsafe.Pointer(&data[0]), C.size_t(len(data)),
		C.int(flags))

	if ret == 0 {
		err = nil
	}
	return err
}

func (fd *Fd) Fremovexattr(attr string) error {

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	ret, err := C.glfs_fremovexattr(fd.fd, cattr)

	if ret == 0 {
		err = nil
	}
	return err
}
