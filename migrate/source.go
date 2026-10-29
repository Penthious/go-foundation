package migrate

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Penthious/go-foundation/internal/schema"
)

type source struct {
	version int64
	name    string
	up      string
	down    string
}

var filenamePattern = regexp.MustCompile(`^([0-9]+)_([A-Za-z0-9][A-Za-z0-9_-]*)\.(up|down)\.sql$`)

func loadSources(cfg Config) ([]source, error) {
	byVersion := make(map[int64]source)
	if cfg.FS != nil {
		entries, err := fs.ReadDir(cfg.FS, cfg.Dir)
		if err != nil {
			return nil, fmt.Errorf("migrate: read directory: %w", err)
		}

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
				continue
			}

			if err := loadFile(cfg, entry.Name(), byVersion); err != nil {
				return nil, err
			}
		}
	}

	for _, builtin := range []source{
		{version: cfg.InboxVersion, name: "gof_inbox", up: schema.Inbox()},
		{version: cfg.OutboxVersion, name: "gof_outbox", up: schema.Outbox()},
		{version: cfg.FailedMessagesVersion, name: "gof_failed_messages", up: schema.FailedMessages()},
		{version: cfg.SchedulerVersion, name: "gof_schedules", up: schema.Scheduler()},
	} {
		if builtin.version == 0 {
			continue
		}

		if _, exists := byVersion[builtin.version]; exists {
			return nil, fmt.Errorf("migrate: duplicate version %d", builtin.version)
		}

		byVersion[builtin.version] = builtin
	}

	sources := make([]source, 0, len(byVersion))
	for _, migration := range byVersion {
		if migration.up == "" {
			return nil, fmt.Errorf("migrate: version %d has no up file", migration.version)
		}

		sources = append(sources, migration)
	}

	if len(sources) == 0 {
		return nil, errors.New("migrate: no migrations configured")
	}

	slices.SortFunc(sources, func(a, b source) int {
		return cmp.Compare(a.version, b.version)
	})

	return sources, nil
}

func loadFile(cfg Config, name string, byVersion map[int64]source) error {
	parts := filenamePattern.FindStringSubmatch(name)
	if parts == nil {
		return fmt.Errorf("migrate: invalid SQL filename %q; use VERSION_name.up.sql or VERSION_name.down.sql", name)
	}

	version, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || version <= 0 {
		return fmt.Errorf("migrate: invalid version in %q", name)
	}

	migration := byVersion[version]
	if migration.name != "" && migration.name != parts[2] {
		return fmt.Errorf("migrate: duplicate version %d with different names", version)
	}

	if (parts[3] == "up" && migration.up != "") || (parts[3] == "down" && migration.down != "") {
		return fmt.Errorf("migrate: duplicate %s file for version %d", parts[3], version)
	}

	body, err := fs.ReadFile(cfg.FS, path.Join(cfg.Dir, name))
	if err != nil {
		return fmt.Errorf("migrate: read %q: %w", name, err)
	}

	if strings.TrimSpace(string(body)) == "" {
		return fmt.Errorf("migrate: empty SQL file %q", name)
	}

	migration.version, migration.name = version, parts[2]
	if parts[3] == "up" {
		migration.up = string(body)
	} else {
		migration.down = string(body)
	}

	byVersion[version] = migration

	return nil
}
