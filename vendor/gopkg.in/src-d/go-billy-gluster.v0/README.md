# go-billy-gluster [![GoDoc](https://godoc.org/gopkg.in/src-d/go-billy-gluster.v0?status.svg)](https://godoc.org/github.com/src-d/go-billy-gluster) [![Build Status](https://travis-ci.org/src-d/go-billy-gluster.svg)](https://travis-ci.org/src-d/go-billy-gluster) [![codecov.io](https://codecov.io/github/src-d/go-billy-gluster/coverage.svg)](https://codecov.io/github/src-d/go-billy-gluster)

This package provides a basic [go-billy](https://github.com/src-d/go-billy) driver to access [gluster](https://www.gluster.org/) volumes. It uses libgfapi so it's not needed to mount the volume locally.

So far, following actions are implemented:

* `Create`
* `Open`
* `OpenFile`
* `Stat`
* `Rename`
* `Remove`
* `MkdirAll`

For more information head to the [documentation](https://godoc.org/gopkg.in/src-d/go-billy-gluster.v0)

# Installation

This package requires native `libgfapi` library that is contained in the development client packages of gluster.

* Arch / Manjaro:

```
pacman -S glusterfs
```

* Ubuntu / Debian:

```
apt-get install glusterfs-common
```

* CentOS / RHEL:

```
yum install glusterfs-api
```

After installing the dependency you can install the library with `go get`:

```
go get gopkg.in/src-d/go-billy-gluster.v0
```

# Example of utilization

```go
package main

import (
	"fmt"

	"gopkg.in/src-d/go-billy-gluster.v0"
)

func main() {
	fs, err := gluster.New("server", "volume")
	if err != nil {
		panic(fmt.Sprintf("cannot connect to gluster volume %s", err))
	}

	f, err := fs.Create("filename.ext")
	if err != nil {
		panic(fmt.Sprintf("cannot create file %s", err))
	}

	_, err = f.Write([]byte("text"))
	if err != nil {
		panic(fmt.Sprintf("cannot write to file %s", err))
	}

	err = f.Close()
	if err != nil {
		panic(fmt.Sprintf("cannot close file %s", err))
	}

	err = fs.Close()
	if err != nil {
		panic(fmt.Sprintf("cannot disconnect from volume %s", err))
	}
}
```

# Contribute

[Contributions](https://github.com/src-d/go-billy-gluster/issues) are more than welcome, if you are interested please take a look to our [Contributing Guidelines](CONTRIBUTING.md).

# Code of Conduct

All activities under source{d} projects are governed by the [source{d} code of conduct](https://github.com/src-d/guide/blob/master/.github/CODE_OF_CONDUCT.md).

# License

Apache License Version 2.0, see [LICENSE](LICENSE).

