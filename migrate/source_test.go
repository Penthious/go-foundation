package migrate

import (
	"database/sql"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"
)

func TestNew_InvalidSources(t *testing.T) {
	file := func(body string) *fstest.MapFile {
		return &fstest.MapFile{Data: []byte(body)}
	}
	for _, tt := range []struct {
		name string
		cfg  Config
	}{
		{name: "empty"},
		{name: "negative timeout", cfg: Config{InboxVersion: 1, Timeout: -time.Second}},
		{name: "negative inbox", cfg: Config{InboxVersion: -1}},
		{name: "negative outbox", cfg: Config{OutboxVersion: -1}},
		{name: "negative failures", cfg: Config{FailedMessagesVersion: -1}},
		{name: "negative scheduler", cfg: Config{SchedulerVersion: -1}},
		{name: "absolute directory", cfg: Config{Dir: "/migrations", InboxVersion: 1}},
		{name: "parent directory", cfg: Config{Dir: "../migrations", InboxVersion: 1}},
		{name: "directory without FS", cfg: Config{Dir: "migrations", InboxVersion: 1}},
		{name: "unversioned SQL", cfg: Config{FS: fstest.MapFS{"orders.sql": file("SELECT 1;")}}},
		{name: "zero version", cfg: Config{FS: fstest.MapFS{"000_orders.up.sql": file("SELECT 1;")}}},
		{name: "overflow version", cfg: Config{FS: fstest.MapFS{"9223372036854775808_orders.up.sql": file("SELECT 1;")}}},
		{name: "down without up", cfg: Config{FS: fstest.MapFS{"1_orders.down.sql": file("SELECT 1;")}}},
		{name: "empty file", cfg: Config{FS: fstest.MapFS{"1_orders.up.sql": file(" \n")}}},
		{name: "different names", cfg: Config{FS: fstest.MapFS{"1_orders.up.sql": file("SELECT 1;"), "1_other.down.sql": file("SELECT 1;")}}},
		{name: "duplicate numeric version", cfg: Config{FS: fstest.MapFS{"1_orders.up.sql": file("SELECT 1;"), "001_orders.up.sql": file("SELECT 1;")}}},
		{name: "built-in collision", cfg: Config{InboxVersion: 1, OutboxVersion: 1}},
		{name: "failure collision", cfg: Config{InboxVersion: 1, FailedMessagesVersion: 1}},
		{name: "scheduler collision", cfg: Config{FailedMessagesVersion: 1, SchedulerVersion: 1}},
		{name: "service collision", cfg: Config{InboxVersion: 1, FS: fstest.MapFS{"1_orders.up.sql": file("SELECT 1;")}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if runner, err := New(&sql.DB{}, tt.cfg); runner != nil || err == nil {
				t.Fatal("invalid migration configuration accepted")
			}
		})
	}

	if _, err := New(nil, Config{InboxVersion: 1}); err == nil {
		t.Fatal("nil database accepted")
	}

	if _, err := New(&sql.DB{}, Config{FS: fstest.MapFS{}, Dir: "missing"}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("filesystem error cause = %v", err)
	}
}

func TestLoadSources_SnapshotAndOrdering(t *testing.T) {
	files := fstest.MapFS{
		"sql/003_orders.up.sql":   {Data: []byte("SELECT 3;")},
		"sql/003_orders.down.sql": {Data: []byte("SELECT -3;")},
		"sql/README.md":           {Data: []byte("ignored")},
	}
	sources, err := loadSources(Config{FS: files, Dir: "sql", InboxVersion: 2, OutboxVersion: 1})
	if err != nil {
		t.Fatal(err)
	}

	files["sql/003_orders.up.sql"].Data[0] = 'X'
	if len(sources) != 3 || sources[0].version != 1 || sources[1].version != 2 || sources[2].version != 3 || sources[2].up != "SELECT 3;" || sources[2].down != "SELECT -3;" {
		t.Fatalf("unexpected snapshot: %+v", sources)
	}
}
