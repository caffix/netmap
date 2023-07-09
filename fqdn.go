// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"time"

	"github.com/owasp-amass/asset-db/types"
	"github.com/owasp-amass/open-asset-model/domain"
	"golang.org/x/net/publicsuffix"
)

// UpsertFQDN adds a fully qualified domain name to the graph.
func (g *Graph) UpsertFQDN(ctx context.Context, name string) (*types.Asset, error) {
	d, err := publicsuffix.EffectiveTLDPlusOne(name)
	if err != nil {
		return nil, err
	}
	_, _ = g.DB.Create(nil, "", &domain.FQDN{Name: d})

	return g.DB.Create(nil, "", &domain.FQDN{Name: name})
}

// UpsertCNAME adds the FQDNs and CNAME record between them to the graph.
func (g *Graph) UpsertCNAME(ctx context.Context, fqdn, target string) error {
	return g.insertAlias(ctx, fqdn, target, "cname_record")
}

// IsCNAMENode returns true if the FQDN has a CNAME edge to another FQDN in the graph.
func (g *Graph) IsCNAMENode(ctx context.Context, fqdn string, since time.Time) bool {
	return g.checkForOutEdge(ctx, fqdn, "cname_record", since)
}

func (g *Graph) insertAlias(ctx context.Context, fqdn, target, relation string) error {
	fAsset, err := g.UpsertFQDN(ctx, fqdn)
	if err != nil {
		return err
	}

	tAsset, err := g.UpsertFQDN(ctx, target)
	if err != nil {
		return err
	}

	_, err = g.DB.Create(fAsset, relation, tAsset.Asset)
	return err
}

// UpsertPTR adds the FQDNs and PTR record between them to the graph.
func (g *Graph) UpsertPTR(ctx context.Context, fqdn, target string) error {
	return g.insertAlias(ctx, fqdn, target, "ptr_record")
}

// IsPTRNode returns true if the FQDN has a PTR edge to another FQDN in the graph.
func (g *Graph) IsPTRNode(ctx context.Context, fqdn string, since time.Time) bool {
	return g.checkForOutEdge(ctx, fqdn, "ptr_record", since)
}

// UpsertSRV adds the FQDNs and SRV record between them to the graph.
func (g *Graph) UpsertSRV(ctx context.Context, fqdn, service, target string) error {
	// Create the edge between the service and the subdomain
	if err := g.insertAlias(ctx, service, fqdn, "srv_record"); err != nil {
		return err
	}
	// Create the edge between the service and the target
	return g.insertAlias(ctx, service, target, "srv_record")
}

// UpsertNS adds the FQDNs and NS record between them to the graph.
func (g *Graph) UpsertNS(ctx context.Context, fqdn, target string) error {
	return g.insertAlias(ctx, fqdn, target, "ns_record")
}

// IsNSNode returns true if the FQDN has a NS edge pointing to it in the graph.
func (g *Graph) IsNSNode(ctx context.Context, fqdn string, since time.Time) bool {
	return g.checkForInEdge(ctx, fqdn, "ns_record", since)
}

// UpsertMX adds the FQDNs and MX record between them to the graph.
func (g *Graph) UpsertMX(ctx context.Context, fqdn, target string) error {
	return g.insertAlias(ctx, fqdn, target, "mx_record")
}

// IsMXNode returns true if the FQDN has a MX edge pointing to it in the graph.
func (g *Graph) IsMXNode(ctx context.Context, fqdn string, since time.Time) bool {
	return g.checkForInEdge(ctx, fqdn, "mx_record", since)
}

func (g *Graph) checkForInEdge(ctx context.Context, id, relation string, since time.Time) bool {
	if assets, err := g.DB.FindByContent(&domain.FQDN{Name: id}, since); err == nil {
		for _, a := range assets {
			if fqdn, ok := a.Asset.(domain.FQDN); ok && fqdn.Name == id {
				if rels, err := g.DB.IncomingRelations(a, since, relation); err == nil && len(rels) > 0 {
					return true
				}
				break
			}
		}
	}
	return false
}

func (g *Graph) checkForOutEdge(ctx context.Context, id, relation string, since time.Time) bool {
	if assets, err := g.DB.FindByContent(&domain.FQDN{Name: id}, since); err == nil {
		for _, a := range assets {
			if fqdn, ok := a.Asset.(domain.FQDN); ok && fqdn.Name == id {
				if rels, err := g.DB.OutgoingRelations(a, since, relation); err == nil && len(rels) > 0 {
					return true
				}
				break
			}
		}
	}
	return false
}
