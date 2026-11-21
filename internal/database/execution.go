package database

import "sync"

// The gate isolates compound commands while allowing audited single-key
// operations to execute concurrently under their database and shard locks.
var executionGate sync.RWMutex

func LockExecution()         { executionGate.Lock() }
func UnlockExecution()       { executionGate.Unlock() }
func LockSharedExecution()   { executionGate.RLock() }
func UnlockSharedExecution() { executionGate.RUnlock() }

func (db *Database) ChangeChannel() <-chan struct{} {
	db.changeMu.Lock()
	defer db.changeMu.Unlock()
	if db.changeCh == nil {
		db.changeCh = make(chan struct{})
	}
	return db.changeCh
}

func (db *Database) notifyChange() {
	db.changeMu.Lock()
	if db.changeCh != nil {
		close(db.changeCh)
		db.changeCh = nil
	}
	db.changeMu.Unlock()
}
