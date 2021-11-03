// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"errors"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

func (g *Graph) Migrate(ctx context.Context, to *Graph) error {
	g.db.Lock()
	defer g.db.Unlock()

	var err error
	var q quad.Quad
	var quads []quad.Quad

	rr := graph.NewResultReader(g.db.store, nil)
	defer rr.Close()

	for err == nil {
		q, err = rr.ReadQuad()
		if err == nil {
			quads = append(quads, q)
		}
	}

	return copyQuads(ctx, to.db, quads)
}

// MigrateEvents copies the nodes and edges related to the Events identified by the uuids from the receiver Graph into another.
func (g *Graph) MigrateEvents(ctx context.Context, to *Graph, uuids ...string) error {
	quads, err := g.ReadEventQuads(ctx, uuids...)

	if err == nil {
		err = copyQuads(ctx, to.db, quads)
	}
	return err
}

// MigrateEventsInScope copies the nodes and edges related to the Events identified by the uuids from the receiver Graph into another.
func (g *Graph) MigrateEventsInScope(ctx context.Context, to *Graph, d []string) error {
	if len(d) == 0 {
		return errors.New("MigrateEventsInScope: No domain names provided")
	}

	var domains []quad.Value
	for _, domain := range d {
		domains = append(domains, quad.IRI(domain))
	}

	vals := make(map[string]struct{})

	g.db.Lock()
	// Obtain the events that are in scope according to the domain name arguments
	p := cayley.StartPath(g.db.store, domains...).Has(quad.IRI("type"), quad.String(TypeFQDN)).SaveReverse(quad.IRI("domain"), "uuid")
	err := p.Iterate(ctx).TagValues(nil, func(m map[string]quad.Value) error {
		vals[valToStr(m["uuid"])] = struct{}{}
		return nil
	})
	g.db.Unlock()
	if err != nil {
		return err
	}

	var uuids []string
	for k := range vals {
		uuids = append(uuids, k)
	}
	return g.MigrateEvents(ctx, to, uuids...)
}

func copyQuads(ctx context.Context, to *CayleyGraph, quads []quad.Quad) error {
	to.Lock()
	defer to.Unlock()

	if len(quads) == 0 {
		return errors.New("copyQuads: No quads provided")
	}

	tx := graph.NewTransactionN(len(quads))
	opts := graph.IgnoreOpts{
		IgnoreMissing: true,
		IgnoreDup:     true,
	}

	for _, q := range quads {
		select {
		case <-ctx.Done():
			return errors.New("copyQuads: context expired")
		default:
		}

		tx.AddQuad(q)
	}

	return to.store.ApplyDeltas(tx.Deltas, opts)
}
