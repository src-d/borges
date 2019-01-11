package borges

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	kallax "gopkg.in/src-d/go-kallax.v1"
	log "gopkg.in/src-d/go-log.v1"
)

// useFast path check if there is only one rooted repository and no previous
// references using it. Also verifies that we can use filesystem and path
// parameters from the temporary repository.
func (a *Archiver) useFastpath(
	l log.Logger,
	c Changes,
	t TemporaryRepository,
) (bool, error) {
	_, ok := t.(*temporaryRepository)
	if ok && len(c) == 1 {
		for k := range c {
			refs, err := a.Store.InitHasRefs(k)
			if err != nil {
				l.Errorf(err, "could not get references from database")
				return false, err
			}

			return !refs, nil
		}
	}

	return false, nil
}

// fastpathRootedRepository skips push and generates a siva file with the
// contents of the temporary repository. It renames references and changes
// configuration before creating the siva file.
func (a *Archiver) fastpathRootedRepository(
	ctx context.Context,
	logger log.Logger,
	r *model.Repository,
	tr TemporaryRepository,
	ic model.SHA1,
) error {
	logger = logger.With(log.Fields{
		"rooted-repository": ic.String(),
	})

	logger.Debugf("using fastpath to create siva file")

	t, ok := tr.(*temporaryRepository)
	if !ok {
		return fmt.Errorf("internal error, not a temporaryRepository")
	}

	repo := t.Repository

	err := renameReferences(repo, r.ID)
	if err != nil {
		return err
	}

	err = StoreConfig(repo, r)
	if err != nil {
		return err
	}

	err = repo.DeleteRemote("origin")
	if err != nil {
		return err
	}

	rootedRepoCpStart := time.Now()
	err = copySivaToRemote(ctx, a, ic, t)
	if err != nil {
		logger.With(log.Fields{
			"duration": time.Since(rootedRepoCpStart),
		}).Errorf(err, "could not copy siva file to FS")
		return err
	}

	logger.With(log.Fields{
		"duration": time.Since(rootedRepoCpStart),
	}).Debugf("copy siva file to FS")

	return nil
}

func rootedRefName(
	name plumbing.ReferenceName,
	id kallax.ULID,
) plumbing.ReferenceName {
	n := fmt.Sprintf("%s/%s", name, id.String())
	return plumbing.ReferenceName(n)
}

func renameReferences(repo *git.Repository, id kallax.ULID) error {
	it, err := repo.References()
	if err != nil {
		return err
	}
	defer it.Close()

	var add []*plumbing.Reference
	var del []plumbing.ReferenceName
	err = it.ForEach(func(ref *plumbing.Reference) error {
		if !strings.HasPrefix(string(ref.Name()), "refs/") {
			return nil
		}

		name := rootedRefName(ref.Name(), id)

		var newRef *plumbing.Reference
		switch ref.Type() {
		case plumbing.HashReference:
			newRef = plumbing.NewHashReference(name, ref.Hash())
		case plumbing.SymbolicReference:
			target := rootedRefName(ref.Target(), id)
			newRef = plumbing.NewSymbolicReference(name, target)
		default:
			return nil
		}

		del = append(del, ref.Name())
		add = append(add, newRef)

		return nil
	})
	if err != nil {
		return err
	}

	for _, name := range del {
		err = repo.Storer.RemoveReference(name)
		if err != nil {
			return err
		}
	}

	for _, ref := range add {
		err = repo.Storer.SetReference(ref)
		if err != nil {
			return err
		}
	}

	return nil
}

func copySivaToRemote(
	ctx context.Context,
	a *Archiver,
	ic model.SHA1,
	t *temporaryRepository,
) error {
	local := a.Copier.Local()
	origPath := fmt.Sprintf("%s.siva", ic.String())
	localPath := local.Join(fmt.Sprintf(
		"%s_%s", ic.String(),
		strconv.FormatInt(time.Now().UnixNano(), 10),
	))
	localSivaPath := filepath.Join(localPath, "siva")
	localTmpPath := filepath.Join(localPath, "tmp")

	defer util.RemoveAll(local, localPath)

	tmpFs, err := local.Chroot(localTmpPath)
	if err != nil {
		return err
	}

	fs, err := sivafs.NewFilesystem(local, localSivaPath, tmpFs)
	if err != nil {
		return err
	}

	err = RecursiveCopy(t.TempPath, t.TempFilesystem, "/", fs)
	if err != nil {
		return err
	}

	err = fs.Sync()
	if err != nil {
		return err
	}

	return a.Copier.CopyToRemote(ctx, localSivaPath, origPath)
}

// RecursiveCopy copies a directory to a destination path. It creates all
// needed directories if destination path does not exist.
func RecursiveCopy(
	src string,
	srcFS billy.Filesystem,
	dst string,
	dstFS billy.Filesystem,
) error {
	stat, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		err = dstFS.MkdirAll(dst, stat.Mode())
		if err != nil {
			return err
		}

		files, err := srcFS.ReadDir(src)
		if err != nil {
			return err
		}

		for _, file := range files {
			srcPath := filepath.Join(src, file.Name())
			dstPath := filepath.Join(dst, file.Name())

			err = RecursiveCopy(srcPath, srcFS, dstPath, dstFS)
			if err != nil {
				return err
			}
		}
	} else {
		err = CopyFile(src, srcFS, dst, dstFS, stat.Mode())
		if err != nil {
			return err
		}
	}

	return nil
}

// CopyFile makes a file copy with the specified permission.
func CopyFile(
	src string,
	srcFS billy.Filesystem,
	dst string,
	dstFS billy.Filesystem,
	mode os.FileMode,
) error {
	_, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	fo, err := srcFS.Open(src)
	if err != nil {
		return err
	}
	defer fo.Close()

	fd, err := dstFS.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = io.Copy(fd, fo)
	if err != nil {
		fd.Close()
		dstFS.Remove(dst)
		return err
	}

	return nil
}
