// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"testing"
)

func TestUpsertSource(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	src := "FakeSource"
	got, err := g.UpsertSource(context.Background(), src)
	if err != nil {
		t.Errorf("Failed to insert source: %v", err)

	}
	if got != src {
		t.Errorf("Expected: %v, Got: %v", src, got)
	}
}

func TestNodeSources(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	event := "eventID"
	ctx := context.Background()
	if _, err := g.NodeSources(ctx, Node(""), event); err == nil {
		t.Errorf("Failed to return an error when provided an invalid node")
	}

	id := "MyNode"
	if _, err := g.NodeSources(ctx, Node(id), event); err == nil {
		t.Errorf("Failed to return an error when provided an non-existent event")
	}

	_, _ = g.UpsertEvent(ctx, event)
	n, _ := g.UpsertNode(ctx, id, TypeFQDN)
	if _, err := g.NodeSources(ctx, n, event); err == nil {
		t.Errorf("Failed to return an error when provided a node with no in-edges")
	}

	srcs := []string{"src1", "src2", "src3", "src4", "src5", "src6", "src7", "src8", "src9", "src10"}
	// Enter the source references
	for _, src := range srcs {
		_ = g.AddNodeToEvent(ctx, n, src, event)
	}

	got, err := g.NodeSources(ctx, n, event)
	if err != nil {
		t.Errorf("Failed to extract sources for the node provided: %v", err)
	}

	if !checkTestResult(srcs, got) {
		t.Errorf("Expected:% v, Got: %v", srcs, got)
	}
}

func TestCacheSourceData(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	src := "fakesource"
	query := "domain.com"
	resp1 := "subssubssubs"
	ctx := context.Background()
	if err := g.CacheSourceData(ctx, src, query, resp1); err != nil {
		t.Errorf("Failed to save the data source response")
	}
	if r, err := g.GetSourceData(ctx, src, query, 1); err != nil || r != resp1 {
		t.Errorf("Failed to extract the data source response from the graph")
	}

	resp2 := "thenewresponse"
	if err := g.CacheSourceData(ctx, src, query, resp2); err != nil {
		t.Errorf("Failed to save the updated data source response")
	}
	if r, err := g.GetSourceData(ctx, src, query, 1); err != nil || r != resp2 {
		t.Errorf("Failed to extract the data source response from the graph")
	}
}
