// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"fmt"

	"github.com/caffix/stringset"
	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

const TypeAddr string = "ipaddr"

// UpsertAddress creates an IP address in the graph and associates it with a source and event.
func (g *Graph) UpsertAddress(ctx context.Context, addr, source, eventID string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.quadsUpsertAddress(t, addr, source, eventID); err != nil {
		return nil, err
	}

	return Node(addr), g.db.applyWithLock(t)
}

func (g *Graph) quadsUpsertAddress(t *graph.Transaction, addr, source, eventID string) error {
	if err := g.db.quadsUpsertNode(t, addr, TypeAddr); err != nil {
		return err
	}
	if err := g.quadsAddNodeToEvent(t, addr, source, eventID); err != nil {
		return err
	}
	return nil
}

// NameAddrPair represents a relationship between a DNS name and an IP address it eventually resolves to.
type NameAddrPair struct {
	Name string
	Addr string
}

var (
	ntype   quad.IRI    = quad.IRI("type")
	cname   quad.IRI    = quad.IRI("cname_record")
	srvrec  quad.IRI    = quad.IRI("srv_record")
	arec    quad.IRI    = quad.IRI("a_record")
	aaaarec quad.IRI    = quad.IRI("aaaa_record")
	fqdn    quad.String = quad.String("fqdn")
)

// NamesToAddrs returns a NameAddrPair for each name / address combination discovered in the graph.
func (g *Graph) NamesToAddrs(ctx context.Context, uuid string, names ...string) ([]*NameAddrPair, error) {
	g.db.Lock()
	defer g.db.Unlock()

	var nameVals []quad.Value
	for _, name := range names {
		nameVals = append(nameVals, quad.IRI(name))
	}

	var filter *stringset.Set
	if len(names) > 0 {
		filter = stringset.New(names...)
		defer filter.Close()
	}

	var nodes *cayley.Path
	event := quad.IRI(uuid)
	eventNode := cayley.StartPath(g.db.store, event)
	nameAddrMap := make(map[string]*stringset.Set, len(names))
	defer func() {
		for _, ss := range nameAddrMap {
			ss.Close()
		}
	}()

	if len(names) > 0 {
		nodes = cayley.StartPath(g.db.store, nameVals...).Tag("name")
	} else {
		d := quad.IRI("domain")
		r := quad.IRI("root")

		domains := eventNode.Out(d).Has(ntype, fqdn).Unique()
		nodes = domains.In(r).Or(domains).Unique().Tag("name")
	}

	set := stringset.New()
	defer set.Close()

	if err := nodes.Iterate(ctx).EachValue(g.db.store.QuadStore, func(v quad.Value) {
		set.Insert(valToStr(v))
	}); err != nil {
		return nil, fmt.Errorf("%s: NamesToAddrs: Failed to iterate over node values: %v", g.String(), err)
	}

	f := addrsCallback(filter, nameAddrMap)
	// Obtain the addresses that are associated with the event and adjacent names
	adj := nodes.Out(arec, aaaarec).Has(ntype, quad.StringToValue(TypeAddr)).Tag("address").In().And(eventNode).Back("name")
	if err := adj.Iterate(ctx).TagValues(nil, f); err != nil {
		return nil, fmt.Errorf("%s: NamesToAddrs: Failed to iterate over tag values: %v", g.String(), err)
	}

	for k := range nameAddrMap {
		set.Remove(k)
	}

	if set.Len() > 0 {
		var vals []quad.Value

		for _, name := range set.Slice() {
			vals = append(vals, quad.IRI(name))
		}

		p := cayley.StartPath(g.db.store, vals...).Tag("name")
		// Get all the nodes for service names and CNAMES
		getSRVsAndCNAMEs(ctx, eventNode, p, f)
	}

	pairs := generatePairsFromAddrMap(nameAddrMap)
	if len(pairs) == 0 {
		return nil, fmt.Errorf("%s: NamesToAddrs: No addresses were discovered", g.String())
	}
	return pairs, nil
}

func addrsCallback(filter *stringset.Set, addrMap map[string]*stringset.Set) func(m map[string]quad.Value) {
	return func(m map[string]quad.Value) {
		name := valToStr(m["name"])
		addr := valToStr(m["address"])

		if filter != nil && !filter.Has(name) {
			return
		}
		if _, found := addrMap[name]; !found {
			addrMap[name] = stringset.New()
		}

		addrMap[name].Insert(addr)
	}
}

func getSRVsAndCNAMEs(ctx context.Context, event, nodes *cayley.Path, f func(m map[string]quad.Value)) {
	p := nodes

	for i := 1; i <= 10; i++ {
		if i == 1 {
			p = p.Out(srvrec, cname)
		} else {
			p = p.Out(cname)
		}

		if count, err := p.Iterate(ctx).Count(); err != nil || count == 0 {
			break
		}

		addrs := p.Out(arec, aaaarec).Has(ntype, quad.StringToValue(TypeAddr)).Tag("address").In().And(event).Back("name")
		if err := addrs.Iterate(ctx).TagValues(nil, f); err != nil {
			break
		}
	}
}

func generatePairsFromAddrMap(addrMap map[string]*stringset.Set) []*NameAddrPair {
	pairs := make([]*NameAddrPair, 0, len(addrMap)*2)

	for name, set := range addrMap {
		for _, addr := range set.Slice() {
			pairs = append(pairs, &NameAddrPair{
				Name: name,
				Addr: addr,
			})
		}
	}

	return pairs
}

// UpsertA creates FQDN, IP address and A record edge in the graph and associates them with a source and event.
func (g *Graph) UpsertA(ctx context.Context, fqdn, addr, source, eventID string) error {
	return g.addrRecord(fqdn, addr, source, eventID, "a_record")
}

// UpsertAAAA creates FQDN, IP address and AAAA record edge in the graph and associates them with a source and event.
func (g *Graph) UpsertAAAA(ctx context.Context, fqdn, addr, source, eventID string) error {
	return g.addrRecord(fqdn, addr, source, eventID, "aaaa_record")
}

func (g *Graph) addrRecord(fqdn, addr, source, eventID, rrtype string) error {
	t := graph.NewTransaction()

	if err := g.quadsUpsertFQDN(t, fqdn, source, eventID); err != nil {
		return err
	}
	if err := g.quadsUpsertAddress(t, addr, source, eventID); err != nil {
		return err
	}
	if err := g.db.quadsUpsertEdge(t, rrtype, fqdn, addr); err != nil {
		return err
	}

	return g.db.applyWithLock(t)
}
