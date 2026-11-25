"""Source patches applied only to disposable detailed replica diagnostic builds."""


DETAIL_PROBES = (
    (
        "\t\tdb.publishView(previous.view, next, status)\n",
        "\t\tphase := db.pollPhaseStart(false)\n"
        "\t\tdb.publishView(previous.view, next, status)\n"
        "\t\tdb.pollPhaseSpan(phase, \"view_publish\", \"\", 0)\n",
    ),
    (
        "\tgeneration, err := os.MkdirTemp(db.directory, \"generation-\")\n",
        "\tphase := db.pollPhaseStart(false)\n"
        "\tgeneration, err := os.MkdirTemp(db.directory, \"generation-\")\n",
    ),
    (
        "\t\tif err := linkPrevious(db.directory, generation, previous, base, !info.Rebase); err != nil {\n",
        "\t\tlink := db.pollPhaseStart(true)\n"
        "\t\terr := linkPrevious(db.directory, generation, previous, base, !info.Rebase)\n"
        "\t\tdb.pollPhaseSpan(link, \"link_previous\", \"\", 0)\n"
        "\t\tif err != nil {\n",
    ),
    (
        "\tfor _, entry := range info.Files {\n"
        "\t\tpath := filepath.Join(generation, entry.Name)\n"
        "\t\tfile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)\n",
        "\tdb.pollPhaseSpan(phase, \"generation_prepare\", \"\", 0)\n"
        "\tphase = db.pollPhaseStart(false)\n"
        "\tfor _, entry := range info.Files {\n"
        "\t\tpart := db.pollPhaseStart(true)\n"
        "\t\tpath := filepath.Join(generation, entry.Name)\n"
        "\t\tfile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)\n"
        "\t\tdb.pollPhaseSpan(part, \"segment_open\", entry.Name, 0)\n",
    ),
    (
        "\t\t_, err = io.CopyN(file, body, entry.Size)\n",
        "\t\tpart = db.pollPhaseStart(true)\n"
        "\t\tcopied, err := io.CopyN(file, body, entry.Size)\n"
        "\t\tdb.pollPhaseSpan(part, \"segment_copy\", entry.Name, copied)\n",
    ),
    (
        "\t\tif err := errors.Join(file.Sync(), file.Close()); err != nil {\n"
        "\t\t\treturn nil, err\n\t\t}\n\t}\n"
        "\tif err := readEnd(body); err != nil {\n",
        "\t\tpart = db.pollPhaseStart(true)\n"
        "\t\tsyncErr := file.Sync()\n"
        "\t\tdb.pollPhaseSpan(part, \"segment_sync\", entry.Name, entry.Size)\n"
        "\t\tpart = db.pollPhaseStart(true)\n"
        "\t\tcloseErr := file.Close()\n"
        "\t\tdb.pollPhaseSpan(part, \"segment_close\", entry.Name, entry.Size)\n"
        "\t\tif err := errors.Join(syncErr, closeErr); err != nil {\n"
        "\t\t\treturn nil, err\n\t\t}\n\t}\n"
        "\tdb.pollPhaseSpan(phase, \"segment_transfer\", \"\", 0)\n"
        "\tphase = db.pollPhaseStart(false)\n"
        "\terr = readEnd(body)\n"
        "\tdb.pollPhaseSpan(phase, \"response_end\", \"\", 0)\n"
        "\tif err != nil {\n",
    ),
    (
        "\tvar next *view\n\tif previous != nil && info.Snapshot == 0 && len(info.Files) > 0 {\n"
        "\t\tnext, err = loadDeltaView(generation, meta, previous.Sequence, info.Files, trace)\n",
        "\tphase = db.pollPhaseStart(false)\n"
        "\tvar next *view\n\tif previous != nil && info.Snapshot == 0 && len(info.Files) > 0 {\n"
        "\t\tnext, err = db.loadDeltaView(generation, meta, previous.Sequence, info.Files, trace)\n",
    ),
    (
        "\tnext.trace = trace\n\ttrace.record(\"go_view_ready\")\n"
        "\tif err := syncDir(generation); err != nil {\n\t\treturn nil, err\n\t}\n"
        "\tif err := syncDir(db.directory); err != nil {\n\t\treturn nil, err\n\t}\n"
        "\tif err := pinSource(info.Source); err != nil {\n",
        "\tnext.trace = trace\n"
        "\tdb.pollPhaseSpan(phase, \"view_prepare\", \"\", 0)\n"
        "\ttrace.record(\"go_view_ready\")\n"
        "\tphase = db.pollPhaseStart(false)\n"
        "\terr = syncDir(generation)\n"
        "\tdb.pollPhaseSpan(phase, \"generation_dir_sync\", \"\", 0)\n"
        "\tif err != nil {\n\t\treturn nil, err\n\t}\n"
        "\tphase = db.pollPhaseStart(false)\n"
        "\terr = syncDir(db.directory)\n"
        "\tdb.pollPhaseSpan(phase, \"database_dir_sync\", \"\", 0)\n"
        "\tif err != nil {\n\t\treturn nil, err\n\t}\n"
        "\tphase = db.pollPhaseStart(false)\n"
        "\terr = pinSource(info.Source)\n"
        "\tdb.pollPhaseSpan(phase, \"pin_source\", \"\", 0)\n"
        "\tif err != nil {\n",
    ),
    (
        "func loadDeltaView(generation string,",
        "func (db *replicaDB) loadDeltaView(generation string,",
    ),
    (
        "\treset, err := rustengine.RecoverGoLogDeltaWithHandleObserver(generation, after, ranges, func(key, raw []byte, deleted bool) error {\n"
        "\t\tvar value *datastruct.DataValue\n",
        "\tvar restore pollPhaseAggregate\n"
        "\tphase := db.pollPhaseStart(false)\n"
        "\treset, err := rustengine.RecoverGoLogDeltaWithHandleObserver(generation, after, ranges, func(key, raw []byte, deleted bool) error {\n"
        "\t\tdecode := db.pollPhaseStart(true)\n"
        "\t\tvar value *datastruct.DataValue\n",
    ),
    (
        "\t\tchanges[string(key)] = value\n\t\treturn nil\n\t}, trace.handleObserver())\n",
        "\t\tchanges[string(key)] = value\n"
        "\t\trestore.add(decode, int64(len(key))+int64(len(raw)))\n"
        "\t\treturn nil\n\t}, trace.handleObserver())\n"
        "\tdb.pollPhaseSpan(phase, \"delta_recovery\", \"\", 0)\n"
        "\tdb.pollPhaseAggregate(\"delta_go_restore\", restore)\n",
    ),
    (
        "func (db *replicaDB) publish(meta installed) (renamed bool, err error) {\n"
        "\tdata, err := json.Marshal(meta)\n",
        "func (db *replicaDB) publish(meta installed) (renamed bool, err error) {\n"
        "\tphase := db.pollPhaseStart(false)\n"
        "\tdata, err := json.Marshal(meta)\n",
    ),
    (
        "\tif err := writeSynced(file, data); err != nil {\n\t\treturn false, err\n\t}\n"
        "\tobserve := db.onCurrentPublish\n",
        "\tpart := db.pollPhaseStart(true)\n"
        "\t_, err = file.Write(data)\n"
        "\tdb.pollPhaseSpan(part, \"current_temp_write\", \"\", int64(len(data)))\n"
        "\tif err != nil {\n\t\tfile.Close()\n\t\treturn false, err\n\t}\n"
        "\tpart = db.pollPhaseStart(true)\n"
        "\tsyncErr := file.Sync()\n"
        "\tdb.pollPhaseSpan(part, \"current_temp_sync\", \"\", int64(len(data)))\n"
        "\tpart = db.pollPhaseStart(true)\n"
        "\tcloseErr := file.Close()\n"
        "\tdb.pollPhaseSpan(part, \"current_temp_close\", \"\", int64(len(data)))\n"
        "\tdb.pollPhaseSpan(phase, \"current_temp_prepare\", \"\", 0)\n"
        "\tif err := errors.Join(syncErr, closeErr); err != nil {\n\t\treturn false, err\n\t}\n"
        "\tobserve := db.onCurrentPublish\n",
    ),
    (
        "\tif err := os.Rename(file.Name(), filepath.Join(db.directory, \"CURRENT\")); err != nil {\n",
        "\tphase = db.pollPhaseStart(false)\n"
        "\terr = os.Rename(file.Name(), filepath.Join(db.directory, \"CURRENT\"))\n"
        "\tdb.pollPhaseSpan(phase, \"current_rename\", \"\", 0)\n"
        "\tif err != nil {\n",
    ),
    (
        "\terr = syncDir(db.directory)\n\tif err == nil {\n"
        "\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"current_durable\", Last: meta.Sequence})\n",
        "\tphase = db.pollPhaseStart(false)\n"
        "\terr = syncDir(db.directory)\n"
        "\tdb.pollPhaseSpan(phase, \"current_final_dir_sync\", \"\", 0)\n"
        "\tif err == nil {\n"
        "\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"current_durable\", Last: meta.Sequence})\n",
    ),
)
