package tool

import (
	"bufio"
	"os"
	"strings"
)

// LoadHashes loads siva hashes from a file and generates a list. The lines
// from the file are parsed so it accepts file lists with bucketing. This list
// is filtered so it does not contain repetitions and is sorted
// n lexicographic order.
func LoadHashes(file string) ([]string, error) {
	return LoadListFilter(file, func(s string) string {
		t := strings.ToLower(s)
		t = strings.TrimSuffix(t, ".siva")
		p := strings.Split(t, "/")

		if len(p) > 1 {
			t = p[len(p)-1]
		}

		return t
	})
}

// LoadList calls LoadListFilter with an empty filter.
func LoadList(file string) ([]string, error) {
	return LoadListFilter(file, nil)
}

// LoadListFilter loads a list of strings, applies a filter function to each
// line, removes duplicates and returns it sorted lexicographically.
func LoadListFilter(file string, filter func(string) string) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	hashes := make(map[string]struct{})

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		t := strings.TrimSpace(scanner.Text())
		if filter != nil {
			t = filter(t)
		}

		if t != "" {
			hashes[t] = struct{}{}
		}
	}

	list := make([]string, 0, len(hashes))
	for s := range hashes {
		list = append(list, s)
	}

	return list, nil
}
