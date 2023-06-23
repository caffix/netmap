// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"golang.org/x/net/publicsuffix"
)

const TypeFQDN string = "fqdn"

// UpsertFQDN adds a fully qualified domain name to the graph.
func (g *Graph) UpsertFQDN(ctx context.Context, name, source, eventID string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.quadsUpsertFQDN(t, name, source, eventID); err != nil {
		return nil, err
	}

	return Node(name), g.db.applyWithLock(t)
}

func (g *Graph) quadsUpsertFQDN(t *graph.Transaction, name, source, eventID string) error {
	tld, _ := publicsuffix.PublicSuffix(name)

	domain, err := publicsuffix.EffectiveTLDPlusOne(name)
	if err != nil {
		return fmt.Errorf("quadsUpsertFQDN: Failed to obtain a valid domain name for %s", name)
	}

	if name == "" || tld == "" || domain == "" {
		return fmt.Errorf("quadsUpsertFQDN: Failed to obtain a valid domain name for %s", name)
	}
	if err := g.db.quadsUpsertNode(t, name, TypeFQDN); err != nil {
		return err
	}
	if err := g.db.quadsUpsertNode(t, domain, TypeFQDN); err != nil {
		return err
	}
	if err := g.db.quadsUpsertNode(t, tld, TypeFQDN); err != nil {
		return err
	}
	// Link the three nodes together
	if err := g.db.quadsUpsertEdge(t, "root", name, domain); err != nil {
		return err
	}
	if err := g.db.quadsUpsertEdge(t, "tld", domain, tld); err != nil {
		return err
	}
	// Source and event edges for the FQDN
	if err := g.quadsAddNodeToEvent(t, name, source, eventID); err != nil {
		return err
	}
	// Source and event edges for the root domain name
	if err := g.quadsAddNodeToEvent(t, domain, source, eventID); err != nil {
		return err
	}
	// Add the domain edge for easy access to the DNS domains in the event
	if err := g.db.quadsUpsertEdge(t, "domain", eventID, domain); err != nil {
		return err
	}
	// Source and event edges for the top-level domain name
	if err := g.quadsAddNodeToEvent(t, tld, source, eventID); err != nil {
		return err
	}

	return nil
}

// UpsertCNAME adds the FQDNs and CNAME record between them to the graph.
func (g *Graph) UpsertCNAME(ctx context.Context, fqdn, target, source, eventID string) error {
	return g.insertAlias(fqdn, target, "cname_record", source, eventID)
}

// IsCNAMENode returns true if the FQDN has a CNAME edge to another FQDN in the graph.
func (g *Graph) IsCNAMENode(ctx context.Context, fqdn string) bool {
	return g.checkForOutEdge(ctx, fqdn, "cname_record")
}

func (g *Graph) insertAlias(fqdn, target, pred, source, eventID string) error {
	t := graph.NewTransaction()

	if err := g.quadsUpsertFQDN(t, fqdn, source, eventID); err != nil {
		return err
	}
	if err := g.quadsUpsertFQDN(t, target, source, eventID); err != nil {
		return err
	}
	if err := g.db.quadsUpsertEdge(t, pred, fqdn, target); err != nil {
		return err
	}

	return g.db.applyWithLock(t)
}

// UpsertPTR adds the FQDNs and PTR record between them to the graph.
func (g *Graph) UpsertPTR(ctx context.Context, fqdn, target, source, eventID string) error {
	return g.insertAlias(fqdn, target, "ptr_record", source, eventID)
}

// IsPTRNode returns true if the FQDN has a PTR edge to another FQDN in the graph.
func (g *Graph) IsPTRNode(ctx context.Context, fqdn string) bool {
	return g.checkForOutEdge(ctx, fqdn, "ptr_record")
}

// UpsertSRV adds the FQDNs and SRV record between them to the graph.
func (g *Graph) UpsertSRV(ctx context.Context, fqdn, service, target, source, eventID string) error {
	// Create the edge between the service and the subdomain
	if err := g.insertAlias(service, fqdn, "service", source, eventID); err != nil {
		return err
	}

	// Create the edge between the service and the target
	return g.insertAlias(service, target, "srv_record", source, eventID)
}

// UpsertNS adds the FQDNs and NS record between them to the graph.
func (g *Graph) UpsertNS(ctx context.Context, fqdn, target, source, eventID string) error {
	return g.insertAlias(fqdn, target, "ns_record", source, eventID)
}

// IsNSNode returns true if the FQDN has a NS edge pointing to it in the graph.
func (g *Graph) IsNSNode(ctx context.Context, fqdn string) bool {
	return g.checkForInEdge(ctx, fqdn, "ns_record")
}

// UpsertMX adds the FQDNs and MX record between them to the graph.
func (g *Graph) UpsertMX(ctx context.Context, fqdn, target, source, eventID string) error {
	return g.insertAlias(fqdn, target, "mx_record", source, eventID)
}

// IsMXNode returns true if the FQDN has a MX edge pointing to it in the graph.
func (g *Graph) IsMXNode(ctx context.Context, fqdn string) bool {
	return g.checkForInEdge(ctx, fqdn, "mx_record")
}

// IsRootDomainNode returns true if the FQDN has a 'root' edge pointing to it in the graph.
func (g *Graph) IsRootDomainNode(ctx context.Context, fqdn string) bool {
	return g.checkForInEdge(ctx, fqdn, "root")
}

// IsTLDNode returns true if the FQDN has a 'tld' edge pointing to it in the graph.
func (g *Graph) IsTLDNode(ctx context.Context, fqdn string) bool {
	return g.checkForInEdge(ctx, fqdn, "tld")
}

func (g *Graph) checkForInEdge(ctx context.Context, id, predicate string) bool {
	if node, err := g.ReadNode(ctx, id, TypeFQDN); err == nil {
		count, err := g.CountInEdges(ctx, node, predicate)

		if err == nil && count > 0 {
			return true
		}
	}

	return false
}

func (g *Graph) checkForOutEdge(ctx context.Context, id, predicate string) bool {
	if node, err := g.ReadNode(ctx, id, TypeFQDN); err == nil {
		count, err := g.CountOutEdges(ctx, node, predicate)

		if err == nil && count > 0 {
			return true
		}
	}

	return false
}
