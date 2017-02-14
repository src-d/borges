package borges

import "github.com/satori/go.uuid"

type mentionJobIter struct{}

// NewMentionJobIter returns a JobIter that returns jobs generated from
// mentions received from a queue (e.g. from rovers).
func NewMentionJobIter() JobIter {
	return &mentionJobIter{}
}

func (i *mentionJobIter) Next() (*Job, error) {
	//TODO: this is still a stub implementation
	return &Job{RepositoryID: uuid.Nil}, nil
}

func (i *mentionJobIter) Close() error {
	//TODO: this is still a stub implementation
	return nil
}
