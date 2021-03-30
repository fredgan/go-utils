package mysql

import (
	"github.com/fredgan/go-utils/sync"
)

type Status struct {
	Querying   sync.AtomicInt64
	QueryCount sync.AtomicUint64
	Executing  sync.AtomicInt64
	ExecCount  sync.AtomicUint64
	ErrCount   sync.AtomicUint64
	UsedTime   sync.AtomicDuration
}
