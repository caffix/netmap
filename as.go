// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"strconv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"golang.org/x/net/context"
)

// TypeAS is the type for an autonomous system in the graph database.
const TypeAS string = "as"

// UpsertAS adds/updates an autonomous system in the graph.
func (g *Graph) UpsertAS(ctx context.Context, asn, desc, source, eventID string) (Node, error) {
	n := Node(asn)
	t := graph.NewTransaction()

	err := g.quadsUpsertAS(ctx, t, asn, desc, source, eventID)
	if err == nil {
		err = g.db.applyWithLock(t)
	}
	return n, err
}

func (g *Graph) quadsUpsertAS(ctx context.Context, t *graph.Transaction, asn, desc, source, eventID string) error {
	if err := g.db.quadsUpsertNode(t, asn, TypeAS); err != nil {
		return err
	}

	if a, err := strconv.Atoi(asn); err == nil {
		// Update the 'desc' property
		if d := g.ReadASDescription(ctx, a); d != "" && d != desc {
			t.RemoveQuad(quad.Make(quad.IRI(asn), quad.IRI("description"), quad.String(d), nil))
		}
	}

	if err := g.db.quadsUpsertProperty(t, asn, "description", desc); err != nil {
		return err
	}

	return g.quadsAddNodeToEvent(t, asn, source, eventID)
}

// UpsertInfrastructure adds/updates an associated IP address, netblock and autonomous system in the graph.
func (g *Graph) UpsertInfrastructure(ctx context.Context, asn int, desc, addr, cidr, source, eventID string) error {
	t := graph.NewTransaction()

	if err := g.quadsUpsertAddress(t, addr, "DNS", eventID); err != nil {
		return err
	}
	if err := g.quadsUpsertNetblock(t, cidr, source, eventID); err != nil {
		return err
	}
	// Create the edge between the CIDR and the address
	if err := g.db.quadsUpsertEdge(t, "contains", cidr, addr); err != nil {
		return err
	}

	asnstr := strconv.Itoa(asn)
	if err := g.quadsUpsertAS(ctx, t, asnstr, desc, source, eventID); err != nil {
		return err
	}
	// Create the edge between the AS and the netblock
	if err := g.db.quadsUpsertEdge(t, "prefix", asnstr, cidr); err != nil {
		return err
	}

	return g.db.applyWithLock(t)
}

// ReadASDescription the description property of an autonomous system in the graph.
func (g *Graph) ReadASDescription(ctx context.Context, asn int) string {
	var result string

	asnstr := strconv.Itoa(asn)
	if p, err := g.ReadProperties(ctx, Node(asnstr), "description"); err == nil && len(p) > 0 {
		result = valToStr(p[0].Value)
	}

	return result
}

func (g *Graph) ReadASPrefixes(ctx context.Context, asn int) []string {
	var prefixes []string

	asnstr := strconv.Itoa(asn)
	if edges, err := g.ReadOutEdges(ctx, Node(asnstr), "prefix"); err == nil {
		for _, edge := range edges {
			prefixes = append(prefixes, g.NodeToID(edge.To))
		}
	}

	return prefixes
}
