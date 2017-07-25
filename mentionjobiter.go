package borges

import (
	"strings"

	"gopkg.in/src-d/core-retrieval.v0/model"
	rmodel "gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/framework.v0/queue"
)

type mentionJobIter struct {
	storer *model.RepositoryStore
	q      queue.Queue
	iter   queue.JobIter
}

// NewMentionJobIter returns a JobIter that returns jobs generated from
// mentions received from a queue (e.g. from rovers).
func NewMentionJobIter(q queue.Queue, storer *model.RepositoryStore) JobIter {
	return &mentionJobIter{
		storer: storer,
		q:      q,
	}
}

func (i *mentionJobIter) Next() (*Job, error) {
	if err := i.initIter(); err != nil {
		return nil, err
	}

	endpoints, j, err := i.getEndpoints()

	if err != nil {
		return nil, err
	}

	ID, err := RepositoryID(endpoints, i.storer)
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
		awnd := 1
		iter, err := i.q.Consume(awnd)
		if err != nil {
			return err
		}

		i.iter = iter
	}

	return nil
}

// getEndpoints obtains the next Job from the queue and decodes the mention on it.
// If success, ALL the endpoints into the mention are returned. Also the job itself is
// returned, to be able to send back the ACK.
func (i *mentionJobIter) getEndpoints() (a []string, j *queue.Job, err error) {
	j, err = i.iter.Next()
	if err != nil {
		return
	}
	var mention rmodel.Mention
	if err = j.Decode(&mention); err != nil {
		return
	}

	as, ok := mention.Context["aliases"]
	if !ok {
		a = []string{mention.Endpoint}

		return
	}

	a = parseAliases(as)

	return
}

func parseAliases(aliases string) []string {
	return strings.Split(aliases, ", ")
}

func (i *mentionJobIter) Close() error {
	if i.iter != nil {
		if err := i.iter.Close(); err != nil {
			return err
		}
	}

	return nil
}
