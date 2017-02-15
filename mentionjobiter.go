package borges

import (
	"srcd.works/core.v0/model"
	"srcd.works/framework.v0/queue"
)

type mentionJobIter struct {
	storer *repositoryStore
	q      queue.Queue
	iter   queue.JobIter
}

// NewMentionJobIter returns a JobIter that returns jobs generated from
// mentions received from a queue (e.g. from rovers).
func NewMentionJobIter(q queue.Queue, storer *model.RepositoryStore) JobIter {
	return &mentionJobIter{
		storer:  &repositoryStore{storer},
		q:      q,
	}
}

func (i *mentionJobIter) Next() (*Job, error) {
	if err := i.initIter(); err != nil {
		return nil, err
	}

	endpoint, j, err := i.getEndpoint()

	if err != nil {
		return nil, err
	}

	ID, err := i.storer.RepositoryID(endpoint)
	if err != nil {
		return nil, err
	}

	bj := &Job{RepositoryID: ID}

	if err := j.Ack(); err != nil {
		return nil, err
	}

	return bj, nil
}

// initIter initialize the iterator if it is not already initialized.
func (i *mentionJobIter) initIter() error {
	if i.iter == nil {
		iter, err := i.q.Consume()
		if err != nil {
			return err
		}

		i.iter = iter
	}

	return nil
}

// getEndpoint obtains the next Job from the queue and decodes the mention on it.
// If success, the endpoint into the mention is returned. Also the job itself is
// returned, to be able to send back the ACK.
func (i *mentionJobIter) getEndpoint() (string, *queue.Job, error) {
	j, err := i.iter.Next()
	if err != nil {
		return "", nil, err
	}
	var mention model.Mention
	if err := j.Decode(&mention); err != nil {
		return "", nil, err
	}
	// TODO normalize mention endpoint
	return mention.Endpoint, j, nil
}

func (i *mentionJobIter) Close() error {
	if i.iter != nil {
		if err := i.iter.Close(); err != nil {
			return err
		}
	}

	return nil
}
