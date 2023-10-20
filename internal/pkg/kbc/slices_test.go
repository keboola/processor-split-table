package kbc

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindSlices(t *testing.T) {
	t.Parallel()

	// Empty dir
	dir := t.TempDir()
	slices, err := FindSlices(dir)
	require.NoError(t, err)
	assert.Len(t, slices, 0)
	size, err := slices.Size()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(0), size)

	// One
	require.NoError(t, os.WriteFile(filepath.Join(dir, "part0001"), []byte("foo"), 0o600))
	slices, err = FindSlices(dir)
	require.NoError(t, err)
	assert.Len(t, slices, 1)
	size, err = slices.Size()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(3), size)

	// Two
	require.NoError(t, os.WriteFile(filepath.Join(dir, "part0002"), []byte("bar"), 0o600))
	slices, err = FindSlices(dir)
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	size, err = slices.Size()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(6), size)

	// Unexpected directory
	subDir := filepath.Join(dir, "sub-dir")
	require.NoError(t, os.Mkdir(subDir, 0o700))
	_, err = FindSlices(dir)
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`unexpected directory "%s"`, subDir), err.Error())
	}
}
