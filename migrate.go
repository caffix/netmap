// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"errors"
	"fmt"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
)

func (g *Graph) Migrate(to *Graph) error {
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

	return copyQuads(to.db, quads)
}

// MigrateEvents copies the nodes and edges related to the Events identified by the uuids from the receiver Graph into another.
func (g *Graph) MigrateEvents(to *Graph, uuids ...string) error {
	quads, err := g.readEventQuads(uuids...)

	if err == nil {
		err = copyQuads(to.db, quads)
	}

	return err
}

// MigrateEventsInScope copies the nodes and edges related to the Events identified by the uuids from the receiver Graph into another.
func (g *Graph) MigrateEventsInScope(to *Graph, d []string) error {
	if len(d) == 0 {
		return errors.New("MigrateEventsInScope: No domain names provided")
	}

	vals := make(map[string]struct{})
	var domains []quad.Value
	for _, domain := range d {
		domains = append(domains, quad.IRI(domain))
	}

	g.db.Lock()
	// Obtain the events that are in scope according to the domain name arguments
	p := cayley.StartPath(g.db.store, domains...).LabelContext(quad.IRI(TypeFQDN)).SaveReverse(quad.IRI("domain"), "uuid")
	err := p.Iterate(context.Background()).TagValues(nil, func(m map[string]quad.Value) {
		vals[valToStr(m["uuid"])] = struct{}{}
	})
	g.db.Unlock()
	if err != nil {
		return err
	}

	var uuids []string
	for k := range vals {
		uuids = append(uuids, k)
	}
	return g.MigrateEvents(to, uuids...)
}

func copyQuads(to *CayleyGraph, quads []quad.Quad) error {
	to.Lock()
	defer to.Unlock()

	opts := make(graph.Options)
	opts["ignore_missing"] = true
	opts["ignore_duplicate"] = true

	if len(quads) == 0 {
		return errors.New("copyQuads: No quads provided")
	}

	w, err := writer.NewSingleReplication(to.store, opts)
	if err != nil {
		return fmt.Errorf("copyQuads: %v", err)
	}
	defer w.Close()

	return w.AddQuadSet(quads)
}
