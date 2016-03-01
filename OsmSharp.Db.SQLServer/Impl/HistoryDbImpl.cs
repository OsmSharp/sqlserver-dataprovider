// The MIT License (MIT)

// Copyright (c) 2016 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

using OsmSharp.Db.Impl;
using System.Data;
using OsmSharp.Changesets;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using OsmSharp.Db.SQLServer.Schema;

namespace OsmSharp.Db.SQLServer.Impl
{
    /// <summary>
    /// A history db implementation.
    /// </summary>
    class HistoryDbImpl : IHistoryDbImpl
    {
        private readonly string _connectionString;

        /// <summary>
        /// Creates a history db.
        /// </summary>
        public HistoryDbImpl(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Creates a history db.
        /// </summary>
        public HistoryDbImpl(SqlConnection connection)
        {
            _connection = connection;

            if (_connection.State == ConnectionState.Open)
            {
                if (!Schema.Tools.HistoryDbDetect(_connection))
                {
                    throw new SqlException("No history schema or incompatible schema detected.");
                }
            }
        }

        private SqlConnection _connection; // Holds the connection to the SQLServer db.

        /// <summary>
        /// Gets the connection.
        /// </summary>
        private SqlConnection GetConnection()
        {
            if (_connection == null)
            {
                _connection = new SqlConnection(_connectionString);
            }
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();

                if (!Schema.Tools.HistoryDbDetect(_connection))
                {
                    throw new SqlException("No history schema or incompatible schema detected.");
                }
            }
            return _connection;
        }

        /// <summary>
        /// Clears all data.
        /// </summary>
        public void Clear()
        {
            this.GetConnection().HistoryDbDeleteAll();
        }

        /// <summary>
        /// Adds an osmGeo.
        /// </summary>
        public void Add(IEnumerable<OsmGeo> osmGeos)
        {
            var connection = this.GetConnection();

            foreach(var osmGeo in osmGeos)
            {
                if (osmGeo.Visible.HasValue && osmGeo.Visible.Value)
                {
                    connection.AddOrUpdate(osmGeo);
                }
                else
                {
                    connection.AddArchive(osmGeo);
                }
            }
        }

        /// <summary>
        /// Gets all visible objects in this database.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<OsmGeo> Get()
        {
            return new Streams.SnapshotDbStreamSource(this.GetConnection());
        }

        /// <summary>
        /// Gets all objects in this database.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<OsmGeo> GetWithArchived()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets all the visible objects for the given keys.
        /// </summary>
        public IEnumerable<OsmGeo> Get(IEnumerable<OsmGeoKey> keys)
        {
            var connection = this.GetConnection();

            var nodes = connection.GetNodesById(keys.Where(x => x.Type == OsmGeoType.Node).Select(x => x.Id));
            var ways = connection.GetWaysById(keys.Where(x => x.Type == OsmGeoType.Way).Select(x => x.Id));
            var relations = connection.GetRelationsById(keys.Where(x => x.Type == OsmGeoType.Relation).Select(x => x.Id));

            return nodes.Cast<OsmGeo>().Concat(
                ways.Cast<OsmGeo>().Concat(
                relations.Cast<OsmGeo>()));
        }

        /// <summary>
        /// Gets all the objects for the given keys.
        /// </summary>
        public IEnumerable<OsmGeo> Get(IEnumerable<OsmGeoVersionKey> keys)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets all the objects for the given keys.
        /// </summary>
        public IEnumerable<OsmGeo> GetWithArchived(IEnumerable<OsmGeoKey> keys)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets all visible objects within the given bounding box.
        /// </summary>
        public IEnumerable<OsmGeo> Get(float minLatitude, float minLongitude, float maxLatitude, float maxLongitude)
        {
            var connection = this.GetConnection();

            // get node ids in the bounding box.
            var nodeIdsInBox = connection.GetNodeIdsInBox(minLatitude, minLongitude, maxLatitude, maxLongitude);
            // get the way ids for the nodes in the bounding box.
            var wayIds = connection.GetWayIdsForNodes(nodeIdsInBox);
            // get the relation ids for the nodes/ways found.
            var relationIds = connection.GetRelationIdsForMembers(nodeIdsInBox, wayIds);

            // make sure the ways are complete by enumerating the missing nodes.
            var ways = connection.GetWaysById(wayIds);
            var missingNodeIds = new HashSet<long>();
            foreach (var way in ways)
            {
                if (way.Nodes != null)
                {
                    foreach (var node in way.Nodes)
                    {
                        if (!nodeIdsInBox.Contains(node) &&
                            !missingNodeIds.Contains(node))
                        {
                            missingNodeIds.Add(node);
                        }
                    }
                }
            }

            // get the actual nodes.
            var nodes = connection.GetNodesById(nodeIdsInBox);
            var missingNodes = connection.GetNodesById(missingNodeIds);

            // get the actual relations.
            var relations = connection.GetRelationsById(relationIds);

            return new Enumerators.MergedEnumerable(
                nodes, missingNodes, ways, relations);
        }

        /// <summary>
        /// Archives the object with the given keys.
        /// </summary>
        public void Archive(IEnumerable<OsmGeoKey> keys)
        {
            var nodes = keys.Where(x => x.Type == OsmGeoType.Node).Select(x => x.Id);
            var ways = keys.Where(x => x.Type == OsmGeoType.Way).Select(x => x.Id);
            var relations = keys.Where(x => x.Type == OsmGeoType.Relation).Select(x => x.Id);

            var connection = this.GetConnection();
            connection.ArchiveNodes(nodes);
            connection.ArchiveWays(ways);
            connection.ArchiveRelations(relations);
        }

        /// <summary>
        /// Gets the last id.
        /// </summary>
        public long GetLastId(OsmGeoType type)
        {
            var connection = this.GetConnection();
            SqlCommand command = null;
            switch (type)
            {
                case OsmGeoType.Node:
                    command = connection.GetCommand("select max(id) from node");
                    break;
                case OsmGeoType.Way:
                    command = connection.GetCommand("select max(id) from way");
                    break;
                case OsmGeoType.Relation:
                    command = connection.GetCommand("select max(id) from relation");
                    break;
            }
            return command.ExecuteScalarLong(-1);
        }

        /// <summary>
        /// Gets the last changeset id.
        /// </summary>
        public long GetLastChangesetId()
        {
            var connection = this.GetConnection();
            var command = connection.GetCommand("select max(id) from changeset");
            return command.ExecuteScalarLong(-1);
        }

        /// <summary>
        /// Adds or updates the given changest.
        /// </summary>
        public void AddOrUpdate(Changeset changeset)
        {
            var connection = this.GetConnection();
            connection.AddOrUpdate(changeset);
        }

        /// <summary>
        /// Adds changes for the given changeset.
        /// </summary>
        public void AddChanges(long id, OsmChange changes)
        {
            var connection = this.GetConnection();
            connection.AddChanges(id, changes);
        }

        /// <summary>
        /// Gets a changeset.
        /// </summary>
        public Changeset GetChangeset(long id)
        {
            return this.GetConnection().GetChangesetById(id);
        }

        /// <summary>
        /// Gets changes for the given changeset.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public OsmChange GetChanges(long id)
        {
            throw new NotImplementedException();
        }
    }
}
