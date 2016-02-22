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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Text;
using OsmSharp.Db;
using OsmSharp.Streams;
using OsmSharp.Changesets;
using System.Data;
using OsmSharp.Db.SQLServer.Tiles;

namespace OsmSharp.Db.SQLServer
{
    /// <summary>
    /// Implements a snapshot db storing a snapshot of OSM-data in an SQLite database.
    /// </summary>
    public class SnapshotDb : ISnapshotDb, IDisposable
    {
        private readonly string _connectionString; // Holds the connection string.
        private readonly bool _createAndDetectSchema; // Flag that indicates if the schema needs to be created if not present.

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(string connectionString)
        {
            _connectionString = connectionString;
            _createAndDetectSchema = false;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(SqlConnection connection)
        {
            _connection = connection;
            _createAndDetectSchema = false;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(SqlConnection connection, bool createSchema)
        {
            _connection = connection;
            _createAndDetectSchema = createSchema;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(string connectionString, bool createSchema)
        {
            _connectionString = connectionString;
            _createAndDetectSchema = createSchema;
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

                Schema.Tools.SnapshotDbCreateAndDetect(_connection);
            }
            return _connection;
        }

        /// <summary>
        /// Gets the sql command.
        /// </summary>
        private SqlCommand GetCommand(string sql)
        {
            return new SqlCommand(sql, this.GetConnection());
        }

        /// <summary>
        /// Gets all the objects in the form of an osm stream source.
        /// </summary>
        public OsmStreamSource Get()
        {
            return new Streams.SnapshotDbStreamSource(this.GetConnection());
        }

        /// <summary>
        /// Returns all the objects within a given bounding box and filtered by a given filter.
        /// </summary>
        public IEnumerable<OsmGeo> Get(float minLatitude, float minLongitude, float maxLatitude, float maxLongitude)
        {
            var boxes = new List<long>();
            var tileRange = TileRange.CreateAroundBoundingBox(minLatitude, minLongitude, maxLatitude, maxLongitude, 14);
            foreach (var tile in tileRange)
            {
                boxes.Add((long)tile.Id);
            }

            // read all nodes in bounding box.               
            var command = this.GetCommand(string.Format(
                "select * from node left outer join node_tags on node.id = node_tags.node_id where (tile in ({0})) AND (latitude >= @minlat AND latitude < @maxlat AND longitude >= @minlon AND longitude < @maxlon) order by id",
                    boxes.BuildCommaSeperated()));
            command.Parameters.AddWithValue("minlat", (long)(minLatitude * 10000000));
            command.Parameters.AddWithValue("maxlat", (long)(maxLatitude * 10000000));
            command.Parameters.AddWithValue("minlon", (long)(minLongitude * 10000000));
            command.Parameters.AddWithValue("maxlon", (long)(maxLongitude * 10000000));
            var reader = command.ExecuteReaderWrapper();
            var nodeIds = new List<long>();
            var nodes = new List<Node>();
            if (reader.Read())
            {
                while (reader.HasActiveRow)
                {
                    var node = reader.BuildNode();
                    nodeIds.Add(node.Id.Value);
                    nodes.Add(node);
                }
            }

            // get ways.
            var extraNodeIds = new List<long>();
            var wayIds = new HashSet<long>();
            command = this.GetCommand(string.Format(
                "select distinct * from way left outer join way_nodes on way.id = way_nodes.way_id where way_id in (select distinct way_id from way join way_nodes on way.id = way_nodes.way_id where node_id in ({0})) order by id, sequence_id", nodeIds.BuildCommaSeperated()));
            reader = command.ExecuteReaderWrapper();
            var ways = new List<Way>();
            if (reader.Read())
            {
                while (reader.HasActiveRow)
                {
                    var way = reader.BuildWay();
                    wayIds.Add(way.Id.Value);
                    ways.Add(way);

                    if (way.Nodes != null)
                    {
                        foreach (var n in way.Nodes)
                        {
                            if (!nodeIds.Contains(n) &&
                                !extraNodeIds.Contains(n))
                            {
                                extraNodeIds.Add(n);
                            }
                        }
                    }
                }
            }

            // get way tags.
            if (wayIds.Count > 0)
            {
                command = this.GetCommand(string.Format(
                    "select * from way_tags where way_id in ({0}) order by way_id", wayIds.BuildCommaSeperated()));
                reader = command.ExecuteReaderWrapper();
                if (reader.Read())
                {
                    var wayEnumerator = ways.GetEnumerator();
                    while (reader.HasActiveRow &&
                        wayEnumerator.MoveNext())
                    {
                        reader.AddTags(wayEnumerator.Current);
                    }
                }
            }

            // get relations.
            var relations = new List<Relation>();
            var relationIds = new List<long>();
            if (nodeIds.Count > 0 || wayIds.Count > 0)
            {
                if (nodeIds.Count > 0 && wayIds.Count > 0)
                {
                    command = this.GetCommand(string.Format(
                        "select * from relation left outer join relation_members on relation.id = relation_members.relation_id where id in (select distinct relation_id from relation_members where (member_id in ({0}) and member_type = 0) or (member_id in ({1}) or member_type = 1)) order by id, sequence_id",
                            nodeIds.BuildCommaSeperated(), wayIds.BuildCommaSeperated()));
                    reader = command.ExecuteReaderWrapper();
                }
                else if(nodeIds.Count > 0)
                {
                    command = this.GetCommand(string.Format(
                        "select * from relation left outer join relation_members on relation.id = relation_members.relation_id where id in (select distinct relation_id from relation_members where (member_id in ({0}) and member_type = 0)) order by id, sequence_id",
                            nodeIds.BuildCommaSeperated()));
                    reader = command.ExecuteReaderWrapper();
                }
                else
                {
                    command = this.GetCommand(string.Format(
                        "select * from relation left outer join relation_members on relation.id = relation_members.relation_id where id in (select distinct relation_id from relation_members where (member_id in ({0}) and member_type = 1)) order by id, sequence_id",
                            wayIds.BuildCommaSeperated()));
                    reader = command.ExecuteReaderWrapper();
                }
                if (reader.Read())
                {
                    while (reader.HasActiveRow)
                    {
                        var relation = reader.BuildRelation();
                        relations.Add(relation);
                        relationIds.Add(relation.Id.Value);
                    }
                }
            }

            // get relation tags.
            if (relationIds.Count > 0)
            {
                command = this.GetCommand(string.Format(
                    "select * from relation_tags where relation_id in ({0}) order by relation_id", relationIds.BuildCommaSeperated()));
                reader = command.ExecuteReaderWrapper();
                if (reader.Read())
                {
                    var relationEnumerator = relations.GetEnumerator();
                    while (reader.HasActiveRow &&
                        relationEnumerator.MoveNext())
                    {
                        reader.AddTags(relationEnumerator.Current);
                    }
                }
            }

            // get extra nodes.
            var extraNodes = this.Get(OsmGeoType.Node, extraNodeIds);
            foreach (var node in extraNodes)
            {
                nodes.Add(node as Node);
            }
            nodes.Sort((x, y) => x.Id.Value.CompareTo(y.Id.Value));

            return nodes.Cast<OsmGeo>().Concat(
                ways.Cast<OsmGeo>()).Concat(
                    relations.Cast<OsmGeo>());
        }

        /// <summary>
        /// Clears all data.
        /// </summary>
        public void Clear()
        {
            Schema.Tools.SnapshotDbDeleteAll(this.GetConnection());
        }

        /// <summary>
        /// Adds or updates the given osm object in the db exactly as given.
        /// </summary>
        /// <remarks>
        /// - Replaces objects that already exist with the given id.
        /// </remarks>
        public void AddOrUpdate(OsmGeo osmGeo)
        {
            if (osmGeo == null) throw new ArgumentNullException();
            if (!osmGeo.Id.HasValue) throw new ArgumentException("Objects without an id cannot be added!");

            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    this.AddOrUpdateNode(osmGeo as Node);
                    break;
                case OsmGeoType.Way:
                    this.AddOrUpdateWay(osmGeo as Way);
                    break;
                case OsmGeoType.Relation:
                    this.AddOrUpdateRelation(osmGeo as Relation);
                    break;
            }
        }

        /// <summary>
        /// Adds or updates osm objects in the db exactly as they are given.
        /// </summary>
        /// <remarks>
        /// - Replaces objects that already exist with the given id.
        /// </remarks>
        public void AddOrUpdate(IEnumerable<OsmGeo> osmGeos)
        {
            foreach (var osmGeo in osmGeos)
            {
                this.AddOrUpdate(osmGeo);
            }
        }

        /// <summary>
        /// Gets an osm object of the given type and the given id.
        /// </summary>
        public OsmGeo Get(OsmGeoType type, long id)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    return this.GetNode(id);
                case OsmGeoType.Way:
                    return this.GetWay(id);
                case OsmGeoType.Relation:
                    return this.GetRelation(id);
            }
            return null;
        }

        /// <summary>
        /// Gets all osm objects with the given types and the given id's.
        /// </summary>
        public IList<OsmGeo> Get(OsmGeoType type, IList<long> id)
        {
            if (id == null) { throw new ArgumentNullException("id"); }

            var result = new List<OsmGeo>();
            for (int i = 0; i < id.Count; i++)
            {
                result.Add(this.Get(type, id[i]));
            }
            return result;
        }

        /// <summary>
        /// Deletes the osm object with the given type, the given id without applying a changeset.
        /// </summary>
        public bool Delete(OsmGeoType type, long id)
        {
            SqlCommand command = null;
            switch (type)
            {
                case OsmGeoType.Node:
                    command = this.GetCommand(
                        string.Format("DELETE FROM node_tags WHERE node_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM node WHERE id IN ({0})",
                            id.ToInvariantString()));
                    break;
                case OsmGeoType.Way:
                    command = this.GetCommand(
                        string.Format("DELETE FROM way_tags WHERE way_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM way_nodes WHERE way_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM way WHERE id IN ({0})",
                            id.ToInvariantString()));
                    break;
                case OsmGeoType.Relation:
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation_tags WHERE relation_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation_members WHERE relation_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation WHERE id IN ({0})",
                            id.ToInvariantString()));
                    break;
            }
            return command.ExecuteNonQuery() > 0;
        }

        /// <summary>
        /// Deletes all osm objects with the given types and the given id's.
        /// </summary>
        public IList<bool> Delete(IList<OsmGeoType> type, IList<long> id)
        {
            if (type == null) { throw new ArgumentNullException("type"); }
            if (id == null) { throw new ArgumentNullException("id"); }
            if (id.Count != type.Count) { throw new ArgumentException("Type and id lists need to have the same size."); }

            var result = new List<bool>();
            for (int i = 0; i < id.Count; i++)
            {
                result.Add(this.Delete(type[i], id[i]));
            }
            return result;
        }

        /// <summary>
        /// Applies the given changeset.
        /// </summary>
        /// <param name="changeset">The changeset to apply.</param>
        /// <param name="bestEffort">When false, it's the entire changeset or nothing. When true the changeset is applied using best-effort.</param>
        /// <returns>The diff result result object containing the diff result and status information.</returns>
        public DiffResultResult ApplyChangeset(OsmChange changeset, bool bestEffort = true)
        {
            if (changeset == null) { throw new ArgumentNullException("changeset"); }
            if (!bestEffort)
            {
                throw new NotSupportedException("A non-best effort strategy is not supported.");
            }

            var diffResults = new List<OsmGeoResult>();

            if (changeset.Create != null &&
                changeset.Create.Length > 0)
            {
                var highestNodeId = this.GetCommand("select max(id) from node").ExecuteScalarLong(0);
                var highestWayId = this.GetCommand("select max(id) from way").ExecuteScalarLong(0);
                var highestRelationId = this.GetCommand("select max(id) from relation").ExecuteScalarLong(0);

                foreach (var create in changeset.Create)
                {
                    var oldId = create.Id;
                    create.Version = 1;
                    switch (create.Type)
                    {
                        case OsmGeoType.Node:
                            highestNodeId++;
                            create.Id = highestNodeId;
                            this.AddOrUpdateNode(create as Node);
                            diffResults.Add(new NodeResult()
                            {
                                NewId = create.Id,
                                NewVersion = 1,
                                OldId = oldId
                            });
                            break;
                        case OsmGeoType.Way:
                            highestWayId++;
                            create.Id = highestWayId;
                            this.AddOrUpdateWay(create as Way);
                            diffResults.Add(new WayResult()
                            {
                                NewId = create.Id,
                                NewVersion = 1,
                                OldId = oldId
                            });
                            break;
                        case OsmGeoType.Relation:
                            highestRelationId++;
                            create.Id = highestRelationId;
                            this.AddOrUpdateRelation(create as Relation);
                            diffResults.Add(new WayResult()
                            {
                                NewId = create.Id,
                                NewVersion = 1,
                                OldId = oldId
                            });
                            break;
                    }
                }
            }

            if (changeset.Modify != null)
            {
                foreach (var modify in changeset.Modify)
                {
                    var oldId = modify.Id;
                    modify.Version = modify.Version + 1;
                    switch (modify.Type)
                    {
                        case OsmGeoType.Node:
                            if (this.AddOrUpdateNode(modify as Node))
                            {
                                diffResults.Add(new NodeResult()
                                {
                                    NewId = oldId,
                                    NewVersion = modify.Version,
                                    OldId = oldId
                                });
                            }
                            break;
                        case OsmGeoType.Way:
                            if (this.AddOrUpdateWay(modify as Way))
                            {
                                diffResults.Add(new WayResult()
                                {
                                    NewId = oldId,
                                    NewVersion = modify.Version,
                                    OldId = oldId
                                });
                            }
                            break;
                        case OsmGeoType.Relation:
                            if (this.AddOrUpdateRelation(modify as Relation))
                            {
                                diffResults.Add(new RelationResult()
                                {
                                    NewId = oldId,
                                    NewVersion = modify.Version,
                                    OldId = oldId
                                });
                            }
                            break;
                    }
                }
            }

            if (changeset.Delete != null)
            {
                foreach (var delete in changeset.Delete)
                {
                    if (this.Delete(delete.Type, delete.Id.Value))
                    {
                        switch (delete.Type)
                        {
                            case OsmGeoType.Node:
                                diffResults.Add(new NodeResult()
                                {
                                    NewId = null,
                                    OldId = delete.Id.Value,
                                    NewVersion = null
                                });
                                break;
                            case OsmGeoType.Way:
                                diffResults.Add(new WayResult()
                                {
                                    NewId = null,
                                    OldId = delete.Id.Value,
                                    NewVersion = null
                                });
                                break;
                            case OsmGeoType.Relation:
                                diffResults.Add(new RelationResult()
                                {
                                    NewId = null,
                                    OldId = delete.Id.Value,
                                    NewVersion = null
                                });
                                break;
                        }
                    }
                }
            }

            return new DiffResultResult(new DiffResult()
            {
                Generator = "OsmSharp",
                Results = diffResults.ToArray(),
                Version = 0.6
            }, DiffResultStatus.BestEffortOK);
        }

        /// <summary>
        /// Disposes of all resources associated with this db.
        /// </summary>
        public void Dispose()
        {
            _connection.Dispose();
        }

        /// <summary>
        /// Adds or updates a node.
        /// </summary>
        private bool AddOrUpdateNode(Node node)
        {
            var wasUpdate = true;
            var cmd = this.GetCommand("delete from node_tags where node_id = @node_id");
            cmd.Parameters.AddWithValue("node_id", node.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("update node set latitude=@latitude, longitude=@longitude, changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, tile=@tile, version=@version, usr=@usr, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
            cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
            cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", node.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.Add("tile", SqlDbType.BigInt).Value = Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id;
            cmd.Parameters.AddWithValue("version", node.Version.Value);
            cmd.Parameters.AddWithValue("usr", node.UserName);
            cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);
            cmd.Parameters.AddWithValue("id", node.Id.Value);

            if (cmd.ExecuteNonQuery() == 0)
            { // oeps, node did not exist, insert.
                cmd = this.GetCommand(
                    "insert into node (id, latitude, longitude, changeset_id, visible, timestamp, tile, version, usr, usr_id) " +
                             "values(@id, @latitude, @longitude, @changeset_id, @visible, @timestamp, @tile, @version, @usr, @usr_id)");
                cmd.Parameters.AddWithValue("id", node.Id.Value);
                cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
                cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
                cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", node.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.Add("tile", SqlDbType.BigInt).Value = Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id;
                cmd.Parameters.AddWithValue("version", node.Version.Value);
                cmd.Parameters.AddWithValue("usr", node.UserName);
                cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (node.Tags != null)
            {
                cmd = this.GetCommand("INSERT into node_tags (node_id, [key], value) VALUES (@node_id, @key, @value)");
                cmd.Parameters.AddWithValue("node_id", node.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in node.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }
            return wasUpdate;
        }

        /// <summary>
        /// Adds or updates a way.
        /// </summary>
        private bool AddOrUpdateWay(Way way)
        {
            var wasUpdate = true;
            var cmd = this.GetCommand("delete from way_tags where way_id = @way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from way_nodes where way_id = @way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "update way set " +
                "changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, " +
                "version=@version, usr=@usr, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", way.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", way.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", way.Version.Value);
            cmd.Parameters.AddWithValue("usr", way.UserName);
            cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = this.GetCommand(
                    "INSERT INTO way (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                       "VALUES(@id, @changeset_id, @visible, @timestamp, @version, @usr, @usr_id)");
                cmd.Parameters.AddWithValue("id", way.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", way.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("version", way.Version.Value);
                cmd.Parameters.AddWithValue("usr", way.UserName);
                cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (way.Tags != null)
            {
                cmd = this.GetCommand("INSERT into way_tags (way_id, [key], value) VALUES (@way_id, @key, @value)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in way.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (way.Nodes != null)
            {
                cmd = this.GetCommand("INSERT into way_nodes (way_id, node_id, sequence_id) VALUES (@way_id, @node_id, @sequence_id)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.Add("node_id", SqlDbType.BigInt);
                cmd.Parameters.Add("sequence_id", SqlDbType.BigInt);
                for (var i = 0; i < way.Nodes.Length; i++)
                {
                    cmd.Parameters["node_id"].Value = way.Nodes[i];
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
            return wasUpdate;
        }

        /// <summary>
        /// Adds or updates a relation.
        /// </summary>
        private bool AddOrUpdateRelation(Relation relation)
        {
            var wasUpdate = true;
            var cmd = this.GetCommand("delete from relation_tags where relation_id = @relation_id");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from relation_members where relation_id = @relation_id");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "update relation set " +
                "changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, " +
                "version=@version, usr=@usr, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", relation.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", relation.Version.Value);
            cmd.Parameters.AddWithValue("usr", relation.UserName);
            cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = this.GetCommand(
                    "INSERT INTO relation (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                       "VALUES(@id, @changeset_id, @visible, @timestamp, @version, @usr, @usr_id)");
                cmd.Parameters.AddWithValue("id", relation.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("version", relation.Version.Value);
                cmd.Parameters.AddWithValue("usr", relation.UserName);
                cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (relation.Tags != null)
            {
                cmd = this.GetCommand("INSERT into relation_tags (relation_id, [key], value) VALUES (@relation_id, @key, @value)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in relation.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (relation.Members != null)
            {
                cmd = this.GetCommand("INSERT into relation_members (relation_id, member_id, member_type, member_role, sequence_id) VALUES (@relation_id, @member_id, @member_type, @member_role, @sequence_id)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("member_id", SqlDbType.BigInt);
                cmd.Parameters.Add("member_type", SqlDbType.Int);
                cmd.Parameters.Add("member_role", SqlDbType.Text);
                cmd.Parameters.Add("sequence_id", SqlDbType.Int);
                for (var i = 0; i < relation.Members.Length; i++)
                {
                    cmd.Parameters["member_id"].Value = relation.Members[i].Id;
                    cmd.Parameters["member_role"].Value = relation.Members[i].Role;
                    cmd.Parameters["member_type"].Value = (int)relation.Members[i].Type;
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
            return wasUpdate;
        }

        /// <summary>
        /// Gets the node with the given id.
        /// </summary>
        private Node GetNode(long id)
        {
            var command = this.GetCommand("SELECT * FROM node WHERE id = @id");
            command.Parameters.AddWithValue("id", id);

            Node node = null;
            using (var reader = command.ExecuteReaderWrapper())
            {
                if (reader.Read())
                {
                    node = reader.BuildNode();
                }
            }
            if (node != null)
            {
                command = this.GetCommand("SELECT * FROM node_tags WHERE node_id = @id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddTags(node);
                    }
                }
            }
            return node;
        }

        /// <summary>
        /// Gets the way with the given id.
        /// </summary>
        private Way GetWay(long id)
        {
            var command = this.GetCommand("SELECT * FROM way WHERE id = @id");
            command.Parameters.AddWithValue("id", id);

            Way way = null;
            using (var reader = command.ExecuteReaderWrapper())
            {
                if (reader.Read())
                {
                    way = reader.BuildWay();
                }
            }
            if (way != null)
            {
                command = this.GetCommand("SELECT * FROM way_nodes WHERE way_id = @id ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddNodes(way);
                    }
                }

                command = this.GetCommand("SELECT * FROM way_tags WHERE way_id = @id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddTags(way);
                    }
                }
            }
            return way;
        }

        /// <summary>
        /// Gets the relation with the given id.
        /// </summary>
        private Relation GetRelation(long id)
        {
            var command = this.GetCommand("SELECT * FROM relation WHERE id = @id");
            command.Parameters.AddWithValue("id", id);

            Relation relation = null;
            using (var reader = command.ExecuteReaderWrapper())
            {
                if (reader.Read())
                {
                    relation = reader.BuildRelation();
                }
            }
            if (relation != null)
            {
                command = this.GetCommand("SELECT * FROM relation_members WHERE relation_id = @id ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddMembers(relation);
                    }
                }

                command = this.GetCommand("SELECT * FROM relation_tags WHERE relation_id = @id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddTags(relation);
                    }
                }
            }
            return relation;
        }
    }
}
