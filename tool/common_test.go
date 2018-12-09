package tool

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenFS(t *testing.T) {
	require := require.New(t)

	tmp, err := ioutil.TempDir("", "borges")
	require.NoError(err)
	defer os.RemoveAll(tmp)

	_, err = OpenFS("invalid:///some/path")
	require.Error(err)

	fs, err := OpenFS(fmt.Sprintf("file://%s", tmp))
	require.NoError(err)

	testFile := filepath.Join(tmp, "test")
	err = ioutil.WriteFile(testFile, []byte("data"), 0660)
	require.NoError(err)

	_, err = fs.Stat("test")
	require.NoError(err)
}

func TestLoadHashes(t *testing.T) {
	require := require.New(t)

	list := `
one
/path/to/two
three.siva

path/to/four.siva

five.nope
one
`

	expectedHashes := []string{
		"one",
		"two",
		"three",
		"four",
		"five.nope",
	}

	expectedList := []string{
		"one",
		"/path/to/two",
		"three.siva",
		"path/to/four.siva",
		"five.nope",
	}

	f, err := writeTmp(list)
	require.NoError(err)
	defer os.Remove(f)

	parsed, err := LoadHashes(f)
	require.NoError(err)
	require.ElementsMatch(expectedHashes, parsed)

	parsed, err = LoadList(f)
	require.NoError(err)
	require.ElementsMatch(expectedList, parsed)
}

func writeTmp(data string) (string, error) {
	f, err := ioutil.TempFile("", "borges")
	if err != nil {
		return "", err
	}

	_, err = f.WriteString(data)
	if err != nil {
		f.Close()
		os.Remove(f.Name())

		return "", err
	}

	err = f.Close()
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}
