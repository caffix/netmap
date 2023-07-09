// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"time"

	"github.com/owasp-amass/asset-db/types"
	"github.com/owasp-amass/open-asset-model/network"
)

// UpsertAS adds/updates an autonomous system in the graph.
func (g *Graph) UpsertAS(ctx context.Context, asn int, desc string) (*types.Asset, error) {

	a, err := g.DB.Create(nil, "", &network.AutonomousSystem{Number: asn})
	if err != nil {
		return nil, err
	}

	_, err = g.DB.Create(a, "managed_by", &network.RIROrganization{Name: desc})
	return a, err
}

// UpsertInfrastructure adds/updates an associated IP address, netblock and autonomous system in the graph.
func (g *Graph) UpsertInfrastructure(ctx context.Context, asn int, desc, addr, cidr string) error {
	ip, err := g.UpsertAddress(ctx, addr)
	if err != nil {
		return err
	}

	netblock, err := g.UpsertNetblock(ctx, cidr)
	if err != nil {
		return err
	}
	// Create the edge between the CIDR and the address
	if _, err := g.DB.Create(netblock, "contains", ip.Asset); err != nil {
		return err
	}

	as, err := g.UpsertAS(ctx, asn, desc)
	if err != nil {
		return err
	}
	// Create the edge between the AS and the netblock
	if _, err := g.DB.Create(as, "announces", netblock.Asset); err != nil {
		return err
	}
	return nil
}

// ReadASDescription the description property of an autonomous system in the graph.
func (g *Graph) ReadASDescription(ctx context.Context, asn int, since time.Time) string {
	assets, err := g.DB.FindByContent(&network.AutonomousSystem{Number: asn}, since)
	if err != nil || len(assets) == 0 {
		return ""
	}

	if rels, err := g.DB.OutgoingRelations(assets[0], since, "managed_by"); err == nil && len(rels) > 0 {
		a, err := g.DB.FindById(rels[0].ToAsset.ID, since)
		if err != nil {
			return ""
		} else if rir, ok := a.Asset.(network.RIROrganization); ok {
			return rir.Name
		}
	}

	return ""
}

func (g *Graph) ReadASPrefixes(ctx context.Context, asn int, since time.Time) []string {
	var prefixes []string

	assets, err := g.DB.FindByContent(&network.AutonomousSystem{Number: asn}, since)
	if err != nil || len(assets) == 0 {
		return prefixes
	}

	if rels, err := g.DB.OutgoingRelations(assets[0], since, "announces"); err == nil && len(rels) > 0 {
		for _, rel := range rels {
			if a, err := g.DB.FindById(rel.ToAsset.ID, since); err != nil {
				continue
			} else if netblock, ok := a.Asset.(network.Netblock); ok {
				prefixes = append(prefixes, netblock.Cidr.String())
			}
		}
	}
	return prefixes
}
