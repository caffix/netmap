// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"fmt"
	"time"

	"github.com/caffix/stringset"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

// TypeSource is the type representing a data source that has contributed findings to the graph.
const TypeSource string = "source"

var notDataSourceSet = stringset.New("tld", "root", "domain",
	"cname_record", "ptr_record", "mx_record", "ns_record", "srv_record", "service")

// UpsertSource creates a data source node in the graph.
func (g *Graph) UpsertSource(source string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.db.quadsUpsertSource(t, source); err != nil {
		return nil, err
	}

	return Node(source), g.db.applyWithLock(t)
}

func (g *CayleyGraph) quadsUpsertSource(t *graph.Transaction, source string) error {
	return g.quadsUpsertNode(t, source, TypeSource)
}

// NodeSources returns the names of data sources that identified the Node parameter during the events.
func (g *Graph) NodeSources(node Node, events ...string) ([]string, error) {
	nstr := g.NodeToID(node)
	if nstr == "" {
		return nil, fmt.Errorf("%s: NodeSources: Invalid node reference argument", g.String())
	}

	allevents, err := g.AllNodesOfType(TypeEvent, events...)
	if err != nil {
		return nil, fmt.Errorf("%s: NodeSources: Failed to obtain the list of events", g.String())
	}

	eventset := stringset.New()
	for _, event := range allevents {
		if estr := g.NodeToID(event); estr != "" {
			eventset.Insert(estr)
		}
	}

	edges, err := g.ReadInEdges(node)
	if err != nil {
		return nil, fmt.Errorf("%s: NodeSources: Failed to obtain the list of in-edges: %v", g.String(), err)
	}

	var sources []string
	filter := stringset.New()
	for _, edge := range edges {
		if notDataSourceSet.Has(edge.Predicate) {
			continue
		}

		if name := g.NodeToID(edge.From); eventset.Has(name) && !filter.Has(edge.Predicate) {
			filter.Insert(edge.Predicate)
			sources = append(sources, edge.Predicate)
		}
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("%s: NodeSources: Failed to discover edges leaving the Node %s", g.String(), nstr)
	}

	return sources, nil
}

// GetSourceData returns the most recent response from the source/tag for the query within the time to live.
func (g *Graph) GetSourceData(source, query string, ttl int) (string, error) {
	var edges []*Edge

	if node, err := g.ReadNode(source, "source"); err == nil {
		edges, _ = g.ReadOutEdges(node, query)
	}

	var data string
	for _, edge := range edges {
		if p, err := g.ReadProperties(edge.To, "timestamp"); err == nil && len(p) > 0 {
			if n := p[0].Value.Native(); n != nil {
				d := time.Duration(ttl) * time.Minute

				if ts, ok := n.(time.Time); !ok || ts.Add(d).Before(time.Now()) {
					continue
				}
			}
		}

		if p, err := g.ReadProperties(edge.To, "response"); err == nil && len(p) > 0 {
			data = valToStr(p[0].Value)
			break
		}
	}

	err := fmt.Errorf("%s: GetSourceData: Failed to obtain a cached response from %s for query %s", g.String(), source, query)
	if data != "" {
		err = nil
	}
	return data, err
}

// CacheSourceData inserts an updated response from the source/tag for the query.
func (g *Graph) CacheSourceData(source, query, resp string) error {
	t := graph.NewTransaction()

	if err := g.db.quadsUpsertSource(t, source); err != nil {
		return err
	}
	// Remove previously cached responses for the same query
	_ = g.deleteCachedData(source, query)

	now := time.Now()
	ts := now.Format(time.RFC3339)
	rnode := source + "-response-" + ts

	if err := g.db.quadsUpsertNode(t, rnode, "response"); err != nil {
		return err
	}
	if err := g.db.quadsUpsertProperty(t, rnode, "timestamp", now); err != nil {
		return err
	}
	if err := g.db.quadsUpsertProperty(t, rnode, "response", resp); err != nil {
		return err
	}
	if err := g.db.quadsUpsertEdge(t, query, source, rnode); err != nil {
		return err
	}

	return g.db.applyWithLock(t)
}

func (g *Graph) deleteCachedData(source, query string) error {
	var edges []*Edge

	if node, err := g.ReadNode(source, "source"); err == nil {
		edges, _ = g.ReadOutEdges(node, query)
	}

	t := graph.NewTransaction()
	for _, edge := range edges {
		if err := g.db.quadsDeleteNode(t, g.NodeToID(edge.To)); err == nil {
			t.RemoveQuad(quad.MakeIRI(g.NodeToID(edge.From), edge.Predicate, g.NodeToID(edge.To), ""))
		}
	}
	return g.db.applyWithLock(t)
}
