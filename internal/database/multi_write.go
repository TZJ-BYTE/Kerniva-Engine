package database

import "fmt"

func (db *Database) HSetMany(key string, fields, values []string) (created int64, err error) {
	if len(fields) != len(values) || len(fields) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'hset' command")
	}
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		return 0, errDatabaseClosed
	}
	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
		if err != nil {
			created = 0
		}
	}()
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if _, duplicate := seen[field]; duplicate {
			continue
		}
		seen[field] = struct{}{}
		exists, readErr := db.hExistsLocked(key, field)
		if readErr != nil {
			return 0, readErr
		}
		if !exists {
			created++
		}
	}
	err = db.hMSetLocked(key, fields, values)
	return
}

func (db *Database) DeleteManyWithError(keys []string) (deleted int64, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		return 0, errDatabaseClosed
	}
	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
		if err != nil {
			deleted = 0
		}
	}()
	for _, key := range keys {
		existed, deleteErr := db.deleteWithErrorLocked(key)
		if deleteErr != nil {
			return 0, deleteErr
		}
		if existed {
			deleted++
		}
	}
	return
}
