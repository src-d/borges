package borges

import "gopkg.in/src-d/core-retrieval.v0/model"

// NewModelReferencer takes a *model.Repository and returns a Referencer that
// accesses its references. The resulting Referencer never returns an error.
func NewModelReferencer(r *model.Repository) Referencer {
	return modelReferencer{r}
}

type modelReferencer struct {
	*model.Repository
}

func (r modelReferencer) References() ([]*model.Reference, error) {
	return r.Repository.References, nil
}
