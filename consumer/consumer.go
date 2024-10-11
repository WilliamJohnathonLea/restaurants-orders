package consumer

import (
	"context"
	"io"
)

type Consumer interface {
	io.Closer
	Consume(context.Context) error
}
