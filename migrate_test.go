// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"testing"

	"github.com/caffix/stringset"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

func TestMigrate(t *testing.T) {
	from := NewGraph(NewCayleyGraphMemory())
	defer from.Close()

	to := NewGraph(NewCayleyGraphMemory())
	defer to.Close()

	ctx := context.Background()
	from.UpsertA(ctx, "www.google.com", "192.168.1.1", "DNS", "event1")
	from.UpsertA(ctx, "www.caffix.net", "10.0.1.1", "DNS", "event2")
	if err := from.Migrate(ctx, to); err != nil {
		t.Errorf("Migration failed to copy graph data")
	}

	fromset := quadStringSet(from)
	defer fromset.Close()

	toset := quadStringSet(to)
	defer toset.Close()

	fromset.Subtract(toset)
	if fromset.Len() != 0 {
		t.Errorf("Migration failed to copy all quads")
	}
}

func quadStringSet(g *Graph) *stringset.Set {
	var err error
	var q quad.Quad
	set := stringset.New()

	rr := graph.NewResultReader(g.db.store, nil)
	defer rr.Close()

	for err == nil {
		q, err = rr.ReadQuad()
		if err == nil {
			set.Insert(q.NQuad())
		}
	}
	return set
}

func TestMigrateEventsInScope(t *testing.T) {
	from := NewGraph(NewCayleyGraphMemory())
	defer from.Close()

	to := NewGraph(NewCayleyGraphMemory())
	defer to.Close()

	ctx := context.Background()
	if err := from.MigrateEventsInScope(ctx, to, nil); err == nil {
		t.Errorf("Failed to report an error when provided no domain for scope")
	}

	from.UpsertA(ctx, "www.google.com", "192.168.1.1", "DNS", "event1")
	from.UpsertA(ctx, "www.caffix.net", "10.0.1.1", "DNS", "event2")
	if err := from.MigrateEventsInScope(ctx, to, []string{"google.com"}); err != nil {
		t.Errorf("Migration failed to copy graph data including the provided domain: %v", err)
	}
	if pairs, err := to.NamesToAddrs(ctx, "event1", "www.google.com"); err != nil || len(pairs) == 0 {
		t.Errorf("The migration failed to copy graph data for the A record in scope")
	}
	if _, err := to.NamesToAddrs(ctx, "event1", "www.caffix.net"); err == nil {
		t.Errorf("The migration copied graph data that was out of scope")
	}
	if events := to.EventList(ctx); len(events) != 1 || events[0] != "event1" {
		t.Errorf("The migration copied events that are out of scope: Expected event1, Got %v", events)
	}
}
