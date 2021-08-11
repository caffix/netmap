// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/quad"
)

func TestInsertEdge(t *testing.T) {
	cay := NewCayleyGraphMemory()
	g := NewGraph(cay)
	defer g.Close()

	bob := "Bob"
	alice := "Alice"
	vBob := quad.IRI(bob)
	vAlice := quad.IRI(alice)
	vType := quad.IRI("type")

	testArgs := []struct {
		Predicate string
		From      string
		To        string
		ErrMsg    string
	}{
		{
			Predicate: "",
			From:      bob,
			To:        alice,
			ErrMsg:    "UpsertEdge returned no error when provided an invalid predicate",
		},
		{
			Predicate: "testing",
			From:      "",
			To:        alice,
			ErrMsg:    "UpsertEdge returned no error when provided an empty 'from' node",
		},
		{
			Predicate: "testing",
			From:      bob,
			To:        "",
			ErrMsg:    "UpsertEdge returned no error when provided an empty 'to' node",
		},
	}

	ctx := context.Background()
	for i, test := range testArgs {
		if i == len(testArgs)-1 {
			_ = g.db.store.AddQuad(quad.Make(vBob, vType, "Person", nil))
		}
		err := g.UpsertEdge(ctx, &Edge{
			Predicate: test.Predicate,
			From:      test.From,
			To:        test.To,
		})
		if err == nil {
			t.Errorf(test.ErrMsg)
		}
	}

	if err := g.db.store.AddQuad(quad.Make(vAlice, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the quad: %v", err)
	}

	err := g.UpsertEdge(ctx, &Edge{
		Predicate: "knows",
		From:      bob,
		To:        alice,
	})
	if err != nil {
		t.Errorf("UpsertEdge failed when given a valid edge with existing nodes")
	}

	// Check if the edge was successfully inserted
	p := cayley.StartPath(g.db.store, vBob).Out(quad.IRI("knows")).Is(vAlice)
	if first, err := p.Iterate(ctx).FirstValue(nil); err != nil || first == nil {
		t.Errorf("UpsertEdge failed to insert the quad for the edge")
	}

	err = g.UpsertEdge(ctx, &Edge{
		Predicate: "knows",
		From:      bob,
		To:        alice,
	})
	if err != nil {
		t.Errorf("UpsertEdge returned an error when attempting to insert an edge for the second time")
	}
}

func TestReadEdges(t *testing.T) {
	cay := NewCayleyGraphMemory()
	g := NewGraph(cay)
	defer g.Close()

	ctx := context.Background()
	if _, err := g.ReadEdges(ctx, ""); err == nil {
		t.Errorf("ReadEdges returned no error when provided an empty node argument")
	}
	if _, err := g.ReadEdges(ctx, "Bob"); err == nil {
		t.Errorf("ReadEdges returned no error when the node does not exist")
	}

	vBob := quad.IRI("Bob")
	vType := quad.IRI("type")
	// setup the initial data in the graph
	if err := g.db.store.AddQuad(quad.Make(vBob, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the Bob quad: %v", err)
	}

	if _, err := g.ReadEdges(ctx, "Bob"); err == nil {
		t.Errorf("ReadEdges returned no error when the node has no edges")
	}

	vAlice := quad.IRI("Alice")
	if err := g.db.store.AddQuad(quad.Make(vAlice, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the Alice quad: %v", err)
	}
	if err := g.db.store.AddQuad(quad.Make(vBob, quad.IRI("knows"), vAlice, nil)); err != nil {
		t.Errorf("Failed to add the Bob knows Alice quad: %v", err)
	}

	if edges, err := g.ReadEdges(ctx, "Bob"); err != nil {
		t.Errorf("ReadEdges returned an error when the node has edges: %v", err)
	} else if len(edges) != 1 || edges[0].Predicate != "knows" || g.NodeToID(edges[0].To) != "Alice" {
		t.Errorf("ReadEdges returned the wrong edges: %v", edges)
	}

	if err := g.db.store.AddQuad(quad.Make(vAlice, quad.IRI("knows"), vBob, nil)); err != nil {
		t.Errorf("Failed to add the Alice knows Bob quad: %v", err)
	}

	if edges, err := g.ReadEdges(ctx, "Bob", "knows"); err != nil {
		t.Errorf("ReadEdges returned an error when the node has multiple edges: %v", err)
	} else if len(edges) != 2 {
		t.Errorf("ReadEdges returned the wrong edges: %v", edges)
	}

	if _, err := g.ReadEdges(ctx, "Bob", "likes"); err == nil {
		t.Errorf("ReadEdges returned no error when the node does not have edges with matching predicates: %v", err)
	}
	if err := g.db.store.AddQuad(quad.Make(vBob, quad.IRI("likes"), vAlice, nil)); err != nil {
		t.Errorf("Failed to add the Bob likes Alice quad: %v", err)
	}

	if edges, err := g.ReadEdges(ctx, "Bob", "likes"); err != nil {
		t.Errorf("ReadEdges returned an error when the node has edges with matching predicates: %v", err)
	} else if len(edges) != 1 || edges[0].Predicate != "likes" || g.NodeToID(edges[0].To) != "Alice" {
		t.Errorf("ReadEdges returned the wrong edges when provided matching predicates: %v", edges)
	}
}

func TestCountEdges(t *testing.T) {
	cay := NewCayleyGraphMemory()
	g := NewGraph(cay)
	defer g.Close()

	ctx := context.Background()
	if count, err := g.CountEdges(ctx, ""); err == nil {
		t.Errorf("CountEdges returned no error when provided an empty node argument")
	} else if count != 0 {
		t.Errorf("CountEdges did not return zero when provided an empty node argument")
	}
	if count, err := g.CountEdges(ctx, "Bob"); err == nil {
		t.Errorf("CountEdges returned no error when the node does not exist")
	} else if count != 0 {
		t.Errorf("CountEdges did not return zero when the node does not exist")
	}
	if count, err := g.CountOutEdges(ctx, "Bob"); err == nil {
		t.Errorf("CountOutEdges returned no error when the node does not exist")
	} else if count != 0 {
		t.Errorf("CountOutEdges did not return zero when the node does not exist")
	}

	vBob := quad.IRI("Bob")
	vType := quad.IRI("type")
	// setup the initial data in the graph
	if err := g.db.store.AddQuad(quad.Make(vBob, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the Bob quad: %v", err)
	}

	if count, err := g.CountEdges(ctx, "Bob"); err != nil {
		t.Errorf("CountEdges returned an error when the node has no edges: %v", err)
	} else if count != 0 {
		t.Errorf("CountEdges returned the wrong count value: %d", count)
	}

	vAlice := quad.IRI("Alice")
	if err := g.db.store.AddQuad(quad.Make(vAlice, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the Alice quad: %v", err)
	}
	if err := g.db.store.AddQuad(quad.Make(vBob, quad.IRI("knows"), vAlice, nil)); err != nil {
		t.Errorf("Failed to add the Bob knows Alice quad: %v", err)
	}

	if count, err := g.CountEdges(ctx, "Bob"); err != nil {
		t.Errorf("CountEdges returned an error when the node has edges: %v", err)
	} else if count != 1 {
		t.Errorf("CountEdges returned the wrong count value: %d", count)
	}

	if err := g.db.store.AddQuad(quad.Make(vAlice, quad.IRI("knows"), vBob, nil)); err != nil {
		t.Errorf("Failed to add the Alice knows Bob quad: %v", err)
	}

	if count, err := g.CountEdges(ctx, "Bob"); err != nil {
		t.Errorf("CountEdges returned an error when the node has multiple edges: %v", err)
	} else if count != 2 {
		t.Errorf("CountEdges returned the wrong count value: %d", count)
	}

	if count, err := g.CountEdges(ctx, "Bob", "likes"); err != nil {
		t.Errorf("CountEdges returned an error when the node does not have edges with matching predicates: %v", err)
	} else if count != 0 {
		t.Errorf("CountEdges returned the wrong count value when the node does not have edges with matching predicates: %d", count)
	}

	if err := g.db.store.AddQuad(quad.Make(vBob, quad.IRI("likes"), vAlice, nil)); err != nil {
		t.Errorf("Failed to add the Bob likes Alice quad: %v", err)
	}

	if count, err := g.CountEdges(ctx, "Bob", "likes"); err != nil {
		t.Errorf("CountEdges returned an error when the node has edges with matching predicates: %v", err)
	} else if count != 1 {
		t.Errorf("CountEdges returned the wrong number of edges when provided matching predicates: %d", count)
	}
}

func TestDeleteEdge(t *testing.T) {
	cay := NewCayleyGraphMemory()
	g := NewGraph(cay)
	defer g.Close()

	bob := "Bob"
	alice := "Alice"
	vBob := quad.IRI(bob)
	vType := quad.IRI("type")

	testArgs := []struct {
		Predicate string
		From      string
		To        string
		ErrMsg    string
	}{
		{
			Predicate: "",
			From:      bob,
			To:        alice,
			ErrMsg:    "DeleteEdge returned no error when provided an invalid predicate",
		},
		{
			Predicate: "testing",
			From:      "",
			To:        alice,
			ErrMsg:    "DeleteEdge returned no error when provided an empty 'from' node",
		},
		{
			Predicate: "testing",
			From:      bob,
			To:        "",
			ErrMsg:    "DeleteEdge returned no error when provided an empty 'to' node",
		},
	}

	ctx := context.Background()
	for i, test := range testArgs {
		if i == len(testArgs)-1 {
			if err := g.db.store.AddQuad(quad.Make(vBob, vType, "Person", nil)); err != nil {
				t.Errorf("Failed to add the Bob quad: %v", err)
			}
		}
		err := g.DeleteEdge(ctx, &Edge{
			Predicate: test.Predicate,
			From:      test.From,
			To:        test.To,
		})
		if err == nil {
			t.Errorf(test.ErrMsg)
		}
	}

	vAlice := quad.IRI(alice)
	if err := g.db.store.AddQuad(quad.Make(vAlice, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the Alice quad: %v", err)
	}

	if err := g.db.store.AddQuad(quad.Make(vBob, quad.IRI("knows"), vAlice, nil)); err != nil {
		t.Errorf("Failed to add the Bob knows Alice quad: %v", err)
	}

	err := g.DeleteEdge(ctx, &Edge{
		Predicate: "knows",
		From:      bob,
		To:        alice,
	})
	if err != nil {
		t.Errorf("DeleteEdge returned an error when provided a valid edge: %v", err)
	}

	// Check if the edge was actually removed
	p := cayley.StartPath(g.db.store, vBob).Out(quad.IRI("knows")).Is(vAlice)
	if first, err := p.Iterate(ctx).FirstValue(nil); err == nil && first != nil {
		t.Errorf("DeleteEdge failed to remove the edge")
	}
}
