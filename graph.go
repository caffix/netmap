// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"embed"
	"fmt"
	"math/rand"
	"os"

	db "github.com/owasp-amass/asset-db"
	pgmigrations "github.com/owasp-amass/asset-db/migrations/postgres"
	sqlitemigrations "github.com/owasp-amass/asset-db/migrations/sqlite3"
	"github.com/owasp-amass/asset-db/repository"
	migrate "github.com/rubenv/sql-migrate"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Graph is the object for managing a network infrastructure link graph.
type Graph struct {
	DB     *db.AssetDB
	dsn    string
	dbtype repository.DBType
}

// NewGraph returns an intialized Graph object.
func NewGraph(system, path string, options string) *Graph {
	g := OpenGraph(system, path, options)
	if g == nil {
		return nil
	}

	var name string
	var fs embed.FS
	var database gorm.Dialector
	switch g.dbtype {
	case repository.SQLite:
		name = "sqlite3"
		fs = sqlitemigrations.Migrations()
		database = sqlite.Open(g.dsn)
	case repository.Postgres:
		name = "postgres"
		fs = pgmigrations.Migrations()
		database = postgres.Open(g.dsn)
	}

	sql, err := gorm.Open(database, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	migrationsSource := migrate.EmbedFileSystemMigrationSource{
		FileSystem: fs,
		Root:       "/",
	}

	_, err = migrate.Exec(sql.DB(), name, migrationsSource, migrate.Up)
	if err != nil {
		panic(err)
	}
	return g
}

// OpenGraph opens and returns a netmap Graph object for the specified database.
func OpenGraph(system, path string, options string) *Graph {
	var dsn string
	var dbtype repository.DBType

	switch system {
	case "memory":
		dbtype = repository.SQLite
		dsn = fmt.Sprintf("file:sqlite%d?mode=memory&cache=shared", rand.Int31n(100))
	case "local":
		dbtype = repository.SQLite
		dsn = path
	case "postgres":
		dbtype = repository.Postgres
		dsn = path
	default:
		return nil
	}

	store := db.New(dbtype, dsn)
	if store == nil {
		return nil
	}

	return &Graph{
		DB:     store,
		dsn:    dsn,
		dbtype: dbtype,
	}
}

func (g *Graph) Remove() {
	switch g.dbtype {
	case repository.SQLite:
		os.Remove(g.dsn)
	case repository.Postgres:
		teardownPostgres(g.dsn)
	}
}

func teardownPostgres(dsn string) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	migrationsSource := migrate.EmbedFileSystemMigrationSource{
		FileSystem: pgmigrations.Migrations(),
		Root:       "/",
	}

	sqlDb, err := db.DB()
	if err != nil {
		panic(err)
	}

	_, err = migrate.Exec(sqlDb, "postgres", migrationsSource, migrate.Down)
	if err != nil {
		panic(err)
	}
}
