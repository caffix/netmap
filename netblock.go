// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
)

const TypeNetblock string = "netblock"

// UpsertNetblock adds a netblock/CIDR to the graph.
func (g *Graph) UpsertNetblock(ctx context.Context, cidr, source, eventID string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.quadsUpsertNetblock(t, cidr, source, eventID); err != nil {
		return nil, err
	}

	return Node(cidr), g.db.applyWithLock(t)
}

func (g *Graph) quadsUpsertNetblock(t *graph.Transaction, cidr, source, eventID string) error {
	if err := g.db.quadsUpsertNode(t, cidr, TypeNetblock); err != nil {
		return err
	}
	if err := g.quadsAddNodeToEvent(t, cidr, source, eventID); err != nil {
		return err
	}
	return nil
}
