// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

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
func (g *Graph) NamesToAddrs(ctx context.Context, names ...string) ([]*NameAddrPair, error) {
	nameAddrMap := make(map[string]*stringset.Set, len(names))
	defer func() {
		for _, ss := range nameAddrMap {
			ss.Close()
		}
	}()

	var fqdns []*types.Asset
	filter := stringset.New()
	for _, name := range names {
		if a, err := g.DB.FindByContent(&domain.FQDN{Name: name}); err == nil && len(a) > 0 {
			if !filter.Has(name) {
				fqdns = append(fqdns, a[0])
				filter.Insert(name)
			}
		}
	}
	filter.Close()

	if len(fqdns) == 0 {
		return nil, errors.New("no names to query")
	}

	type target struct {
		fqdn  *domain.FQDN
		asset *types.Asset
	}
	var targets []*target
	// Obtain the assets that could have address relations
	for _, a := range fqdns {
		if fqdn, ok := a.Asset.(domain.FQDN); ok {
			cur := a
			// Get to the end of the alias chains for service names and CNAMES
			for i := 1; i <= 10; i++ {
				reltypes := []string{"cname_record"}
				if i == 1 {
					reltypes = append(reltypes, "srv_record")
				}

				if rels, err := g.DB.OutgoingRelations(cur, reltypes...); err == nil && len(rels) > 0 {
					for _, rel := range rels {
						if found, err := g.DB.FindById(rel.ToAsset.ID); err == nil {
							cur = found
							break
						}
					}
				} else {
					break
				}
			}

			targets = append(targets, &target{
				fqdn:  &fqdn,
				asset: cur,
			})
		}
	}

	if len(targets) == 0 {
		return nil, errors.New("no targets to query")
	}

	for _, tar := range targets {
		if rels, err := g.DB.OutgoingRelations(tar.asset, "a_record", "aaaa_record"); err == nil && len(rels) > 0 {
			name := tar.fqdn.Name

			for _, rel := range rels {
				if _, found := nameAddrMap[name]; !found {
					nameAddrMap[name] = stringset.New()
				}

				found, err := g.DB.FindById(rel.ToAsset.ID)
				if err != nil {
					continue
				}

				if a, ok := found.Asset.(network.IPAddress); ok {
					nameAddrMap[name].Insert(a.Address.String())
				}
			}
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
