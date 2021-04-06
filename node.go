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
func (g *CayleyGraph) NodeToID(n Node) string {
	return n.(string)
}

// AllNodesOfType implements the GraphDatabase interface.
func (g *CayleyGraph) AllNodesOfType(ntypes ...string) ([]Node, error) {
	g.Lock()
	defer g.Unlock()

	var types []quad.Value
	for _, t := range ntypes {
		types = append(types, quad.String(t))
	}

	var nodes []Node
	filter := stringset.New()
	p := cayley.StartPath(g.store).Has(quad.IRI("type"), types...)
	err := p.Iterate(context.Background()).EachValue(nil, func(value quad.Value) {
		nstr := valToStr(value)

		if !filter.Has(nstr) {
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
func (g *CayleyGraph) AllOutNodes(node Node) ([]Node, error) {
	g.Lock()
	defer g.Unlock()

	var nodes []Node
	filter := stringset.New()
	p := cayley.StartPath(g.store, quad.IRI(g.NodeToID(node))).Out().Has(quad.IRI("type"))
	err := p.Iterate(context.Background()).EachValue(nil, func(value quad.Value) {
		nstr := valToStr(value)

		if !filter.Has(nstr) {
			filter.Insert(nstr)
			nodes = append(nodes, nstr)
		}
	})

	if err == nil && len(nodes) == 0 {
		return nodes, fmt.Errorf("%s: AllOutNodes: No nodes found that %s has out edges to", g.String(), node)
	}
	return nodes, err
}

// UpsertNode implements the GraphDatabase interface.
func (g *CayleyGraph) UpsertNode(id, ntype string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.quadsUpsertNode(t, id, ntype); err != nil {
		return nil, err
	}

	return Node(id), g.applyWithLock(t)
}

func (g *CayleyGraph) quadsUpsertNode(t *graph.Transaction, id, ntype string) error {
	if id == "" || ntype == "" {
		return fmt.Errorf("%s: quadsUpsertNode: Empty required arguments", g.String())
	}

	t.AddQuad(quad.Make(quad.IRI(id), quad.IRI("type"), quad.String(ntype), quad.IRI(ntype)))
	return nil
}

// ReadNode implements the GraphDatabase interface.
func (g *CayleyGraph) ReadNode(id, ntype string) (Node, error) {
	g.Lock()
	defer g.Unlock()

	if id == "" || ntype == "" {
		return nil, fmt.Errorf("%s: ReadNode: Empty required arguments", g.String())
	}

	// Check that a node with 'id' as a subject already exists
	if !g.nodeExists(id, ntype) {
		return nil, fmt.Errorf("%s: ReadNode: Node %s does not exist", g.String(), id)
	}

	return id, nil
}

// DeleteNode implements the GraphDatabase interface.
func (g *CayleyGraph) DeleteNode(node Node) error {
	id := g.NodeToID(node)
	if id == "" {
		return fmt.Errorf("%s: DeleteNode: Empty node id provided", g.String())
	}

	// Check that a node with 'id' as a subject already exists
	if !g.nodeExists(id, "") {
		return fmt.Errorf("%s: DeleteNode: Node %s does not exist", g.String(), id)
	}

	t := cayley.NewTransaction()
	// Build the transaction that will perform the deletion
	if err := g.quadsDeleteNode(t, id); err != nil {
		return err
	}
	// Attempt to perform the deletion transaction
	return g.applyWithLock(t)
}

func (g *CayleyGraph) quadsDeleteNode(t *graph.Transaction, id string) error {
	g.Lock()
	defer g.Unlock()

	p := cayley.StartPath(g.store, quad.IRI(id)).Tag(
		"subject").BothWithTags([]string{"predicate"}).Tag("object")
	err := p.Iterate(context.TODO()).TagValues(nil, func(m map[string]quad.Value) {
		t.RemoveQuad(quad.Make(m["subject"], m["predicate"], m["object"], nil))
	})
	if err != nil {
		return fmt.Errorf("%s: quadsDeleteNode: Failed to iterate over %s tags: %v", g.String(), id, err)
	}

	return nil
}

// WriteNodeQuads replicates nodes from the cg parameter to the receiver CayleyGraph.
func (g *CayleyGraph) WriteNodeQuads(cg *CayleyGraph, nodes []Node) error {
	cg.Lock()
	defer cg.Unlock()

	var nodeValues []quad.Value
	for _, node := range nodes {
		nodeValues = append(nodeValues, quad.IRI(cg.NodeToID(node)))
	}

	var quads []quad.Quad
	p := cayley.StartPath(cg.store, nodeValues...).Tag("subject").OutWithTags([]string{"predicate"}).Tag("object")
	err := p.Iterate(context.TODO()).TagValues(nil, func(m map[string]quad.Value) {
		var label quad.Value
		if valToStr(m["predicate"]) == "type" {
			label = quad.IRI(valToStr(m["object"]))
		}
		quads = append(quads, quad.Make(m["subject"], m["predicate"], m["object"], label))
	})
	if err != nil {
		return fmt.Errorf("%s: WriteNodeQuads: Failed to iterate over node tags: %v", g.String(), err)
	}

	return copyQuads(g, quads)
}

func (g *CayleyGraph) nodeExists(id, ntype string) bool {
	p := cayley.StartPath(g.store, quad.IRI(id))

	if ntype == "" {
		p = p.Has(quad.IRI("type"))
	} else {
		p = p.Has(quad.IRI("type"), quad.String(ntype))
	}

	var found bool
	if first, err := p.Iterate(context.Background()).FirstValue(nil); err == nil && first != nil {
		found = true
	}

	return found
}
