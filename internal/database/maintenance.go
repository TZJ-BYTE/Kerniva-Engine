package database

func (db *Database) runMaintenance() {
	if db.rustEngine == nil {
		return
	}
	// Engine owns its handle lifetime lock and permits readers during compaction.
	if _, err := db.rustEngine.MaybeCompact(); err != nil {
		db.recordLSMError(err)
	}
	if err := db.persistenceCheckpoint(); err != nil {
		db.recordLSMError(err)
	}
}
