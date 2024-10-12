package db

import (
	"github.com/gocraft/dbr/v2"
)

func WithTx(tx *dbr.Tx, fn func() error) error {
	defer tx.RollbackUnlessCommitted()
	err := fn()
	if err != nil {
		return err
	}
	return tx.Commit()
}
