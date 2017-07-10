package borges

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/core-retrieval.v0/model"
)

func TestNewModelReferencer(t *testing.T) {
	for _, ct := range ChangesFixtures {
		t.Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)
			refs := NewModelReferencer(&model.Repository{References: ct.NewReferences})
			res, err := refs.References()
			require.NoError(err)
			require.Equal(ct.NewReferences, res)
		})
	}
}
