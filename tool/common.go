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
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	hashes := make(map[string]struct{})

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		t := strings.TrimSpace(strings.ToLower(scanner.Text()))
		t = strings.TrimSuffix(t, ".siva")
		p := strings.Split(t, "/")

		if len(p) > 1 {
			t = p[len(p)-1]
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
