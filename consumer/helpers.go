package consumer

import (
	"github.com/gocraft/dbr/v2"
)

func withTx(tx *dbr.Tx, fn func() error) error {
	defer tx.RollbackUnlessCommitted()
	err := fn()
	if err != nil {
		return err
	}
	return tx.Commit()
}
