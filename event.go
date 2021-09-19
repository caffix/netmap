// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/caffix/stringset"
	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"golang.org/x/net/publicsuffix"
)

// TypeEvent is the type that represents an event over a range of time that extended the graph.
const TypeEvent string = "event"

// UpsertEvent create an event node in the graph that represents a discovery task.
func (g *Graph) UpsertEvent(ctx context.Context, eventID string) (Node, error) {
	t := graph.NewTransaction()

	if err := g.quadsUpsertEvent(t, eventID); err != nil {
		return nil, err
	}

	return Node(eventID), g.db.applyWithLock(t)
}

func (g *Graph) quadsUpsertEvent(t *graph.Transaction, eventID string) error {
	if err := g.db.quadsUpsertNode(t, eventID, TypeEvent); err != nil {
		return err
	}

	g.eventFinishLock.Lock()
	defer g.eventFinishLock.Unlock()

	curTime := time.Now()
	delta := 5 * time.Second
	finish, ok := g.eventFinishes[eventID]
	if !ok {
		if err := g.db.quadsUpsertProperty(t, eventID, "start", time.Now()); err != nil {
			return err
		}
	}
	// Remove an existing 'finish' property and enter a new one every 5 seconds
	if ok && (curTime.Sub(finish) > delta) {
		t.RemoveQuad(quad.Make(quad.IRI(eventID), quad.IRI("finish"), finish, nil))
	}
	if !ok || (curTime.Sub(finish) > delta) {
		finish = curTime

		// Update the finish property with the current time/date
		if err := g.db.quadsUpsertProperty(t, eventID, "finish", finish); err != nil {
			return err
		}

		g.eventFinishes[eventID] = finish
	}

	return nil
}

// AddNodeToEvent creates associations between a node in the graph, a data source and a discovery task.
func (g *Graph) AddNodeToEvent(ctx context.Context, node Node, source, eventID string) error {
	t := graph.NewTransaction()

	if err := g.quadsAddNodeToEvent(t, g.NodeToID(node), source, eventID); err != nil {
		return err
	}

	return g.db.applyWithLock(t)
}

func (g *Graph) quadsAddNodeToEvent(t *graph.Transaction, node, source, eventID string) error {
	if node == "" || source == "" || eventID == "" {
		return errors.New("Graph: AddNodeToEvent: Invalid arguments provided")
	}

	if err := g.quadsUpsertEvent(t, eventID); err != nil {
		return err
	}
	if err := g.db.quadsUpsertSource(t, source); err != nil {
		return err
	}
	if err := g.db.quadsUpsertEdge(t, "used", eventID, source); err != nil {
		return err
	}
	if err := g.db.quadsUpsertEdge(t, source, eventID, node); err != nil {
		return err
	}

	return nil
}

// InEventScope checks if the Node parameter is within scope of the Event identified by the uuid parameter.
func (g *Graph) InEventScope(ctx context.Context, node Node, uuid string, predicates ...string) bool {
	if edges, err := g.ReadInEdges(ctx, node, predicates...); err == nil {
		for _, edge := range edges {
			if g.NodeToID(edge.From) == uuid {
				return true
			}
		}
	}
	return false
}

// EventsInScope returns the events that include all of the domain arguments.
func (g *Graph) EventsInScope(ctx context.Context, d ...string) []string {
	g.db.Lock()
	defer g.db.Unlock()

	var domains []quad.Value
	for _, domain := range d {
		domains = append(domains, quad.IRI(domain))
	}

	var events []string
	p := cayley.StartPath(g.db.store, domains...).In(quad.IRI("domain")).Unique()
	_ = p.Iterate(ctx).EachValue(nil, func(value quad.Value) {
		events = append(events, valToStr(value))
	})

	return events
}

// EventList returns a list of event UUIDs found in the graph.
func (g *Graph) EventList(ctx context.Context) []string {
	var events []string

	if nodes, err := g.AllNodesOfType(ctx, TypeEvent); err == nil {
		ids := stringset.New()
		defer ids.Close()

		for _, node := range nodes {
			n := g.NodeToID(node)

			if !ids.Has(n) {
				ids.Insert(n)
				events = append(events, n)
			}
		}
	}

	return events
}

// EventFQDNs returns the domains that were involved in the event.
func (g *Graph) EventFQDNs(ctx context.Context, uuid string) []string {
	names := stringset.New()
	defer names.Close()

	if domains := g.EventDomains(ctx, uuid); domains != nil {
		names.InsertMany(domains...)
	}
	if subs := g.EventSubdomains(ctx, uuid); subs != nil {
		names.InsertMany(subs...)
	}

	return names.Slice()
}

// EventDomains returns the domains that were involved in the event.
func (g *Graph) EventDomains(ctx context.Context, uuid string) []string {
	event, err := g.ReadNode(ctx, uuid, TypeEvent)
	if err != nil {
		return nil
	}

	domains := stringset.New()
	defer domains.Close()

	if edges, err := g.ReadOutEdges(ctx, event, "domain"); err == nil {
		for _, edge := range edges {
			if d := g.NodeToID(edge.To); d != "" {
				domains.Insert(d)
			}
		}
	}

	return domains.Slice()
}

// EventSubdomains returns the subdomains discovered during the event(s).
func (g *Graph) EventSubdomains(ctx context.Context, events ...string) []string {
	nodes, err := g.AllNodesOfType(ctx, TypeFQDN, events...)
	if err != nil {
		return nil
	}

	var names []string
	for _, n := range nodes {
		d := g.NodeToID(n)
		etld, err := publicsuffix.EffectiveTLDPlusOne(d)
		if err != nil || etld == d {
			continue
		}

		names = append(names, g.NodeToID(n))
	}

	return names
}

// EventDateRange returns the date range associated with the provided event UUID.
func (g *Graph) EventDateRange(ctx context.Context, uuid string) (time.Time, time.Time) {
	var start, finish time.Time

	if event, err := g.ReadNode(ctx, uuid, TypeEvent); err == nil {
		if properties, err := g.ReadProperties(ctx, event, "start", "finish"); err == nil {
			for _, p := range properties {
				if t := p.Value.Native(); t != nil && p.Predicate == "start" {
					start = t.(time.Time)
				} else {
					finish = t.(time.Time)
				}
			}
		}
	}

	return start, finish
}

// ReadEventQuads returns all graph database quads associated with the provided events.
func (g *Graph) ReadEventQuads(ctx context.Context, uuids ...string) ([]quad.Quad, error) {
	g.db.Lock()
	defer g.db.Unlock()

	var events []quad.Value
	for _, event := range uuids {
		events = append(events, quad.IRI(event))
	}

	var quads []quad.Quad
	nodeMap := make(map[string]quad.Value)
	// Build quads for the events in scope
	p := cayley.StartPath(g.db.store, events...).Has(quad.IRI("type"), quad.String(TypeEvent))
	p = p.Tag("subject").OutWithTags([]string{"predicate"}).Tag("object")
	err := p.Iterate(ctx).TagValues(nil, func(m map[string]quad.Value) {
		if isIRI(m["object"]) {
			nodeMap[valToStr(m["object"])] = m["object"]
		}

		var label quad.Value
		if valToStr(m["predicate"]) == "type" {
			label = quad.IRI(valToStr(m["object"]))
		}

		quads = append(quads, quad.Make(m["subject"], m["predicate"], m["object"], label))
	})
	if err != nil {
		return nil, fmt.Errorf("MigrateEvents: Failed to iterate over the events: %v", err)
	}

	var nodes []quad.Value
	for _, v := range nodeMap {
		nodes = append(nodes, v)
	}

	// Build quads for all nodes associated with the events in scope
	p = cayley.StartPath(g.db.store, nodes...).Has(quad.IRI("type"))
	p = p.Tag("subject").OutWithTags([]string{"predicate"}).Tag("object")
	err = p.Iterate(ctx).TagValues(nil, func(m map[string]quad.Value) {
		var label quad.Value
		if valToStr(m["predicate"]) == "type" {
			label = quad.IRI(valToStr(m["object"]))
		}

		quads = append(quads, quad.Make(m["subject"], m["predicate"], m["object"], label))
	})
	if err != nil {
		return nil, fmt.Errorf("MigrateEvents: Failed to iterate over the event nodes: %v", err)
	}

	return quads, nil
}
