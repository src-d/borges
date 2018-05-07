package repository

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	require := require.New(t)
	content := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	var buf bytes.Buffer

	err := copy(context.TODO(), &buf, bytes.NewBuffer(content))
	require.NoError(err)
	require.Equal(content, buf.Bytes())

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	buf.Reset()
	err = copy(ctx, &buf, bytes.NewBuffer(content))
	require.Error(err)
	require.True(ErrCopyCancelled.Is(err))
	require.Equal([]byte{}, buf.Bytes())
}
