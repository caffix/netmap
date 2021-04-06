// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

// Graph implements the network infrastructure data model.
type Graph struct {
	db            *CayleyGraph
	alreadyClosed bool

	// eventFinishes maintains a cache of the latest finish time for each event
	// This reduces roundtrips to the graph when adding nodes to events.
	eventFinishes   map[string]time.Time
	eventFinishLock sync.Mutex
}

// NewGraph accepts a graph database that stores the Graph created and maintained by the data model.
func NewGraph(database *CayleyGraph) *Graph {
	if database == nil {
		return nil
	}

	return &Graph{
		db:            database,
		eventFinishes: make(map[string]time.Time),
	}
}

// Close will close the graph database being used by the Graph receiver.
func (g *Graph) Close() {
	if !g.alreadyClosed {
		g.alreadyClosed = true
		g.db.Close()
	}
}

// String returns the name of the graph database used by the Graph.
func (g *Graph) String() string {
	return g.db.String()
}

// UpsertNode will create a node in the database.
func (g *Graph) UpsertNode(id, ntype string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.db.quadsUpsertNode(t, id, ntype); err != nil {
		return nil, err
	}

	return Node(id), g.db.applyWithLock(t)
}

// UpsertEdge will create an edge in the database if it does not already exist.
func (g *Graph) UpsertEdge(edge *Edge) error {
	t := graph.NewTransaction()

	from := g.db.NodeToID(edge.From)
	to := g.db.NodeToID(edge.To)
	if err := g.db.quadsUpsertEdge(t, edge.Predicate, from, to); err != nil {
		return err
	}

	return g.db.applyWithLock(t)
}

// ReadNode returns the node matching the id and type arguments.
func (g *Graph) ReadNode(id, ntype string) (Node, error) {
	return g.db.ReadNode(id, ntype)
}

// AllNodesOfType provides all nodes in the graph of the identified
// type within the optionally identified events.
func (g *Graph) AllNodesOfType(ntype string, events ...string) ([]Node, error) {
	var nodes []Node

	for _, id := range g.nodeIDsOfType(ntype, events...) {
		if node, err := g.db.ReadNode(id, ntype); err == nil {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) == 0 {
		return nil, errors.New("Graph: AllNodesOfType: No nodes found")
	}
	return nodes, nil
}

func isIRI(val quad.Value) bool {
	_, ok := val.(quad.IRI)

	return ok
}

func (g *Graph) nodeIDsOfType(ntype string, events ...string) []string {
	g.db.Lock()
	defer g.db.Unlock()

	var eventVals []quad.Value
	for _, event := range events {
		eventVals = append(eventVals, quad.IRI(event))
	}

	p := cayley.StartPath(g.db.store, eventVals...)
	if ntype != "event" {
		p = p.Out()
	}

	var ids []string
	p = p.Has(quad.IRI("type"), quad.String(ntype)).Unique()
	_ = p.Iterate(context.Background()).EachValue(nil, func(value quad.Value) {
		ids = append(ids, valToStr(value))
	})

	return ids
}

// DumpGraph prints all data currently in the graph.
func (g *Graph) DumpGraph() string {
	return g.db.DumpGraph()
}
