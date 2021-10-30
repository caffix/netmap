// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"fmt"

	"github.com/caffix/stringset"
	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

// Node represents a node in the graph.
type Node interface{}

// NodeToID implements the GraphDatabase interface.
func (g *Graph) NodeToID(n Node) string {
	return n.(string)
}

// AllNodesOfType provides all nodes in the graph of the identified
// type within the optionally identified events.
func (g *Graph) AllNodesOfType(ctx context.Context, ntype string, uuids ...string) ([]Node, error) {
	g.db.Lock()
	defer g.db.Unlock()

	var events []quad.Value
	for _, uuid := range uuids {
		events = append(events, quad.IRI(uuid))
	}

	var p *cayley.Path
	if ntype != TypeEvent && len(events) > 0 {
		p = cayley.StartPath(g.db.store, events...).Has(quad.IRI("type"), quad.String(TypeEvent)).Out().Has(quad.IRI("type"), quad.String(ntype))
	} else {
		p = cayley.StartPath(g.db.store).Has(quad.IRI("type"), quad.String(ntype))
	}

	var nodes []Node
	filter := stringset.New()
	defer filter.Close()

	err := p.Iterate(ctx).EachValue(nil, func(value quad.Value) {
		if nstr := valToStr(value); !filter.Has(nstr) {
			filter.Insert(nstr)
			nodes = append(nodes, nstr)
		}
	})

	if err == nil && len(nodes) == 0 {
		return nodes, fmt.Errorf("%s: AllNodesOfType: No nodes found", g.String())
	}
	return nodes, err
}

// AllOutNodes returns all the nodes that the parameter node has out edges to.
func (g *Graph) AllOutNodes(ctx context.Context, node Node) ([]Node, error) {
	g.db.Lock()
	defer g.db.Unlock()

	var nodes []Node
	filter := stringset.New()
	defer filter.Close()

	p := cayley.StartPath(g.db.store, quad.IRI(g.NodeToID(node))).Out().Has(quad.IRI("type"))
	err := p.Iterate(ctx).EachValue(nil, func(value quad.Value) {
		if nstr := valToStr(value); !filter.Has(nstr) {
			filter.Insert(nstr)
			nodes = append(nodes, nstr)
		}
	})

	if err == nil && len(nodes) == 0 {
		return nodes, fmt.Errorf("%s: AllOutNodes: No nodes found that %s has out edges to", g.String(), node)
	}
	return nodes, err
}

// UpsertNode will create a node in the database.
func (g *Graph) UpsertNode(ctx context.Context, id, ntype string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.db.quadsUpsertNode(t, id, ntype); err != nil {
		return nil, err
	}

	return Node(id), g.db.applyWithLock(t)
}

func (g *CayleyGraph) quadsUpsertNode(t *graph.Transaction, id, ntype string) error {
	if id == "" || ntype == "" {
		return fmt.Errorf("%s: quadsUpsertNode: Empty required arguments", g.String())
	}

	t.AddQuad(quad.Make(quad.IRI(id), quad.IRI("type"), quad.String(ntype), quad.IRI(ntype)))
	return nil
}

// ReadNode returns the node matching the id and type arguments.
func (g *Graph) ReadNode(ctx context.Context, id, ntype string) (Node, error) {
	g.db.Lock()
	defer g.db.Unlock()

	if id == "" || ntype == "" {
		return nil, fmt.Errorf("%s: ReadNode: Empty required arguments", g.String())
	}

	// Check that a node with 'id' as a subject already exists
	if !g.db.nodeExists(ctx, id, ntype) {
		return nil, fmt.Errorf("%s: ReadNode: Node %s does not exist", g.String(), id)
	}

	return id, nil
}

// DeleteNode implements the GraphDatabase interface.
func (g *Graph) DeleteNode(ctx context.Context, node Node) error {
	g.db.Lock()
	defer g.db.Unlock()

	id := g.NodeToID(node)
	if id == "" {
		return fmt.Errorf("%s: DeleteNode: Empty node id provided", g.String())
	}

	// Check that a node with 'id' as a subject already exists
	if !g.db.nodeExists(ctx, id, "") {
		return fmt.Errorf("%s: DeleteNode: Node %s does not exist", g.String(), id)
	}

	t := cayley.NewTransaction()
	// Build the transaction that will perform the deletion
	if err := g.db.quadsDeleteNode(ctx, t, id); err != nil {
		return err
	}
	// Attempt to perform the deletion transaction
	return g.db.store.ApplyTransaction(t)
}

func (g *CayleyGraph) quadsDeleteNode(ctx context.Context, t *graph.Transaction, id string) error {
	p := cayley.StartPath(g.store, quad.IRI(id)).Tag(
		"subject").BothWithTags([]string{"predicate"}).Tag("object")
	err := p.Iterate(ctx).TagValues(nil, func(m map[string]quad.Value) {
		t.RemoveQuad(quad.Make(m["subject"], m["predicate"], m["object"], nil))
	})
	if err != nil {
		return fmt.Errorf("%s: quadsDeleteNode: Failed to iterate over %s tags: %v", g.String(), id, err)
	}

	return nil
}

// WriteNodeQuads replicates nodes from the cg parameter to the receiver Graph.
func (g *Graph) WriteNodeQuads(ctx context.Context, cg *Graph, nodes []Node) error {
	cg.db.Lock()
	defer cg.db.Unlock()

	var nodeValues []quad.Value
	for _, node := range nodes {
		nodeValues = append(nodeValues, quad.IRI(cg.NodeToID(node)))
	}

	var quads []quad.Quad
	p := cayley.StartPath(cg.db.store, nodeValues...).Tag("subject").OutWithTags([]string{"predicate"}).Tag("object")
	err := p.Iterate(ctx).TagValues(nil, func(m map[string]quad.Value) {
		var label quad.Value
		if valToStr(m["predicate"]) == "type" {
			label = quad.IRI(valToStr(m["object"]))
		}
		quads = append(quads, quad.Make(m["subject"], m["predicate"], m["object"], label))
	})
	if err != nil {
		return fmt.Errorf("%s: WriteNodeQuads: Failed to iterate over node tags: %v", g.String(), err)
	}

	return copyQuads(ctx, g.db, quads)
}

func (g *CayleyGraph) nodeExists(ctx context.Context, id, ntype string) bool {
	p := cayley.StartPath(g.store, quad.IRI(id))

	if ntype == "" {
		p = p.Has(quad.IRI("type"))
	} else {
		p = p.Has(quad.IRI("type"), quad.String(ntype))
	}

	var found bool
	if first, err := p.Iterate(ctx).FirstValue(nil); err == nil && first != nil {
		found = true
	}

	return found
}
