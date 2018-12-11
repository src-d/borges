package tool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	require := require.New(t)

	list := []string{
		"one",
		"two",
		"three",
		"one",
		"two",
		"one",
		"four",
	}

	set := NewSet(false)

	for _, s := range list {
		require.False(set.Contains(s))
	}

	for _, s := range list {
		set.Add(s)
	}

	for _, s := range list {
		require.True(set.Contains(s))
	}

	require.False(set.Contains("five"))

	expected := []string{
		"four",
		"one",
		"three",
		"two",
	}

	require.Equal(expected, set.List())
}
