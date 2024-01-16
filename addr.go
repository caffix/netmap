// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/caffix/stringset"
	"github.com/owasp-amass/asset-db/types"
	"github.com/owasp-amass/open-asset-model/domain"
	"github.com/owasp-amass/open-asset-model/network"
)

// UpsertAddress creates an IP address in the graph.
func (g *Graph) UpsertAddress(ctx context.Context, addr string) (*types.Asset, error) {
	ip, err := netip.ParseAddr(addr)
	if err != nil {
		return nil, err
	}

	var t string
	if ip.Is4() {
		t = "IPv4"
	} else if ip.Is6() {
		t = "IPv6"
	} else {
		return nil, fmt.Errorf("%s is not a valid IPv4 or IPv6 IP address", addr)
	}

	return g.DB.Create(nil, "", &network.IPAddress{
		Address: ip,
		Type:    t,
	})
}

// NameAddrPair represents a relationship between a DNS name and an IP address it eventually resolves to.
type NameAddrPair struct {
	FQDN *domain.FQDN
	Addr *network.IPAddress
}

// NamesToAddrs returns a NameAddrPair for each name / address combination discovered in the graph.
func (g *Graph) NamesToAddrs(ctx context.Context, since time.Time, names ...string) ([]*NameAddrPair, error) {
	nameAddrMap := make(map[string]*stringset.Set, len(names))
	defer func() {
		for _, ss := range nameAddrMap {
			ss.Close()
		}
	}()

	remaining := stringset.New()
	defer remaining.Close()
	remaining.InsertMany(names...)

	from := "(relations inner join assets on relations.from_asset_id = assets.id)"
	where := "where assets.type = 'FQDN' and relations.type in ('a_record','aaaa_record') "
	likeset := "and assets.content->>'name' in ('" + strings.Join(remaining.Slice(), "','") + "')"
	query := from + where + likeset
	if !since.IsZero() {
		query += " and relations.last_seen > " + since.Format("2006-01-02 15:04:05")
	}

	rels, err := g.DB.RelationQuery(query)
	if err != nil {
		return nil, err
	} else if len(rels) == 0 {
		return nil, errors.New("no names to query")
	}

	for _, rel := range rels {
		if f, ok := rel.FromAsset.Asset.(*domain.FQDN); ok {
			remaining.Remove(f.Name)

			if _, found := nameAddrMap[f.Name]; !found {
				nameAddrMap[f.Name] = stringset.New()
			}
			if a, ok := rel.ToAsset.Asset.(*network.IPAddress); ok {
				nameAddrMap[f.Name].Insert(a.Address.String())
			}
		}
	}

	// Get to the end of the CNAME alias chains
	for _, name := range remaining.Slice() {
		var results []struct {
			Name string
			Addr string
		}

		if err := g.DB.RawQuery(cnameQuery(name, since), &results); err == nil && len(results) > 0 {
			remaining.Remove(name)

			for _, res := range results {
				if _, found := nameAddrMap[name]; !found {
					nameAddrMap[name] = stringset.New()
				}
				nameAddrMap[name].Insert(res.Addr)
			}
		}
	}

	query = `SELECT fqdns.content->>'name',ips.content->>'name' FROM ((((
		assets AS fqdns INNER JOIN relations AS r1 ON fqdns.id = r1.from_asset_id) 
		INNER JOIN assets AS srvs ON r1.to_asset_id = srvs.id) 
		INNER JOIN relations AS r2 ON srvs.id = r2.from_asset_id) 
		INNER JOIN assets AS ips ON r2.to_asset_id = ips.id)
		WHERE fqdns.type = 'FQDN' AND srvs.type = 'FQDN' AND ips.type = 'IPAddress' 
		AND r1.type = 'srv_record' AND r2.type IN ('a_record', 'aaaa_record')`
	if !since.IsZero() {
		query += " AND r1.last_seen > " + since.Format("2006-01-02 15:04:05") +
			" AND r2.last_seen > " + since.Format("2006-01-02 15:04:05")
	}
	query += " AND fqdns.content->>'name' in ('" + strings.Join(remaining.Slice(), "','") + "')"

	var results []struct {
		Name string
		Addr string
	}
	// Get to the IPs associated with SRV records
	if err := g.DB.RawQuery(query, &results); err == nil && len(results) > 0 {
		for _, res := range results {
			remaining.Remove(res.Name)
			if _, found := nameAddrMap[res.Name]; !found {
				nameAddrMap[res.Name] = stringset.New()
			}
			nameAddrMap[res.Name].Insert(res.Addr)
		}
	}

	if len(nameAddrMap) == 0 {
		return nil, errors.New("no pairs to process")
	}

	pairs := generatePairsFromAddrMap(nameAddrMap)
	if len(pairs) == 0 {
		return nil, errors.New("no addresses were discovered")
	}
	return pairs, nil
}

func cnameQuery(name string, since time.Time) string {
	query := `WITH RECURSIVE
	traverse_cname(fqdn) AS (
	VALUES('` + name + `')
	UNION
	SELECT cnames.content->>'name' FROM ((assets AS fqdns
	INNER JOIN relations ON fqdns.id = relations.from_asset_id) 
	INNER JOIN assets AS cnames ON relations.to_asset_id = cnames.id), traverse_cname
	WHERE fqdns.type = 'FQDN' AND cnames.type = 'FQDN'`
	if !since.IsZero() {
		query += " and relations.last_seen > " + since.Format("2006-01-02 15:04:05")
	}
	query += ` AND relations.type = 'cname_record' AND fqdns.content->>'name' = traverse_cname.fqdn
	)
	SELECT fqdns.content->>'name', ips.content->>'address' FROM ((assets AS fqdns
	INNER JOIN relations ON fqdns.id = relations.from_asset_id) 
	INNER JOIN assets AS ips ON relations.to_asset_id = ips.id)
	WHERE fqdns.type = 'FQDN' AND ips.type = 'IPAddress'`
	if !since.IsZero() {
		query += " and relations.last_seen > " + since.Format("2006-01-02 15:04:05")
	}
	return query + ` AND relations.type IN ('a_record', 'aaaa_record') AND fqdns.content->>'name' IN traverse_cname`
}

func generatePairsFromAddrMap(addrMap map[string]*stringset.Set) []*NameAddrPair {
	var pairs []*NameAddrPair

	for name, set := range addrMap {
		for _, addr := range set.Slice() {
			if ip, err := netip.ParseAddr(addr); err == nil {
				address := &network.IPAddress{Address: ip}
				if ip.Is4() {
					address.Type = "IPv4"
				} else if ip.Is6() {
					address.Type = "IPv6"
				}
				pairs = append(pairs, &NameAddrPair{
					FQDN: &domain.FQDN{Name: name},
					Addr: address,
				})
			}
		}
	}
	return pairs
}

// UpsertA creates FQDN, IP address and A record edge in the graph and associates them with a source and event.
func (g *Graph) UpsertA(ctx context.Context, fqdn, addr string) error {
	return g.addrRecord(ctx, fqdn, addr, "a_record")
}

// UpsertAAAA creates FQDN, IP address and AAAA record edge in the graph and associates them with a source and event.
func (g *Graph) UpsertAAAA(ctx context.Context, fqdn, addr string) error {
	return g.addrRecord(ctx, fqdn, addr, "aaaa_record")
}

func (g *Graph) addrRecord(ctx context.Context, fqdn, addr, rrtype string) error {
	name, err := g.UpsertFQDN(ctx, fqdn)
	if err != nil {
		return err
	}

	ip, err := g.UpsertAddress(ctx, addr)
	if err != nil {
		return err
	}

	_, err = g.DB.Create(name, rrtype, ip.Asset)
	return err
}
