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
using OsmSharp.Streams;
using OsmSharp.Changesets;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using OsmSharp.Db.SQLServer.Tiles;

namespace OsmSharp.Db.SQLServer
{
    /// <summary>
    /// Implements a history db storing a full-history snapshot of OSM-data in an SQL-server database.
    /// </summary>
    public class HistoryDb : IHistoryDb, IDisposable
    {
        private readonly string _connectionString; // Holds the connection string.
        private readonly bool _createAndDetectSchema; // Flag that indicates if the schema needs to be created if not present.

        /// <summary>
        /// Creates a history db.
        /// </summary>
        public HistoryDb(string connectionString)
        {
            _connectionString = connectionString;
            _createAndDetectSchema = false;
        }

        /// <summary>
        /// Creates a history db.
        /// </summary>
        public HistoryDb(SqlConnection connection)
        {
            _connection = connection;
            _createAndDetectSchema = false;
        }

        /// <summary>
        /// Creates a history db.
        /// </summary>
        public HistoryDb(SqlConnection connection, bool createSchema)
        {
            _connection = connection;
            _createAndDetectSchema = createSchema;
        }

        /// <summary>
        /// Creates a history db.
        /// </summary>
        public HistoryDb(string connectionString, bool createSchema)
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

                Schema.Tools.HistoryDbCreateAndDetect(_connection);
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
            return new Streams.HistoryDbStreamSource(this.GetConnection());
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
                "select * from node left outer join node_tags on node.id = node_tags.node_id where (tile in ({0})) AND (visible=1) AND (latitude >= @minlat AND latitude < @maxlat AND longitude >= @minlon AND longitude < @maxlon) order by id",
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

            // get relations.
            command = this.GetCommand(string.Format(
                "select * from relation left outer join relation_members on relation.id = relation_members.relation_id where id in (select distinct relation_id from relation_members where (member_id in ({0}) and member_type = 0) or (member_id in ({1}) or member_type = 1)) order by id, sequence_id",
                    nodeIds.BuildCommaSeperated(), wayIds.BuildCommaSeperated()));
            reader = command.ExecuteReaderWrapper();
            var relations = new List<Relation>();
            var relationIds = new List<long>();
            if (reader.Read())
            {
                while (reader.HasActiveRow)
                {
                    var relation = reader.BuildRelation();
                    relations.Add(relation);
                    relationIds.Add(relation.Id.Value);
                }
            }

            // get relation tags.
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

            // get extra nodes.
            var extraNodes = this.Get(OsmGeoType.Node, extraNodeIds);
            foreach (var node in extraNodes)
            {
                if (node != null)
                {
                    nodes.Add(node as Node);
                }
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
            Schema.Tools.HistoryDbDeleteAll(this.GetConnection());
        }

        /// <summary>
        /// Adds or updates the given osm object in the db exactly as given.
        /// </summary>
        public void Add(OsmGeo osmGeo)
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
        public void Add(IEnumerable<OsmGeo> osmGeos)
        {
            foreach (var osmGeo in osmGeos)
            {
                this.Add(osmGeo);
            }
        }

        /// <summary>
        /// Adds the given changeset in the db exactly as given.
        /// </summary>
        public void Add(Changeset changeset, OsmChange osmChange)
        {
            var cmd = this.GetCommand("delete from changeset_tags where changeset_id = @changeset_id");
            cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from changeset_changes where changeset_id = @changeset_id");
            cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "update changeset set " +
                "closed_at=@closed_at, created_at=@created_at, " +
                "min_lat=@min_lat, min_lon=@min_lon, max_lat=@max_lat, max_lon=@max_lon, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", changeset.Id.Value);
            cmd.Parameters.AddWithValue("closed_at", changeset.ClosedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("created_at", changeset.CreatedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("min_lat", changeset.MinLatitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("min_lon", changeset.MinLongitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("max_lat", changeset.MaxLatitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("max_lon", changeset.MaxLongitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("usr_id", changeset.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = this.GetCommand(
                    "INSERT INTO changeset (id, closed_at, created_at, min_lat, min_lon, max_lat, max_lon, usr_id) " +
                       "VALUES(@id, @closed_at, @created_at, @min_lat, @min_lon, @max_lat, @max_lon, @usr_id)");
                cmd.Parameters.AddWithValue("id", changeset.Id.Value);
                cmd.Parameters.AddWithValue("closed_at", changeset.ClosedAt.ToUnixTimeDB());
                cmd.Parameters.AddWithValue("created_at", changeset.CreatedAt.ToUnixTimeDB());
                cmd.Parameters.AddWithValue("min_lat", changeset.MinLatitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("min_lon", changeset.MinLongitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("max_lat", changeset.MaxLatitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("max_lon", changeset.MaxLongitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("usr_id", changeset.UserId.Value);

                cmd.ExecuteNonQuery();
            }

            if (changeset.Tags != null)
            {
                cmd = this.GetCommand("INSERT into changeset_tags (changeset_id, [key], value) VALUES (@changeset_id, @key, @value)");
                cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in changeset.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (osmChange != null)
            {
                if (osmChange.Create != null)
                {
                    foreach (var create in osmChange.Create)
                    {
                        cmd = this.GetCommand(
                            "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                               "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                        cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
                        cmd.Parameters.AddWithValue("type", Constants.CreateType);
                        cmd.Parameters.AddWithValue("osm_id", create.Id.Value);
                        cmd.Parameters.AddWithValue("osm_type", (int)create.Type);
                        cmd.Parameters.AddWithValue("osm_version", create.Version.Value);

                        cmd.ExecuteNonQuery();
                    }
                }

                if (osmChange.Delete != null)
                {
                    foreach (var delete in osmChange.Delete)
                    {
                        cmd = this.GetCommand(
                            "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                               "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                        cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
                        cmd.Parameters.AddWithValue("type", Constants.DeleteType);
                        cmd.Parameters.AddWithValue("osm_id", delete.Id.Value);
                        cmd.Parameters.AddWithValue("osm_type", (int)delete.Type);
                        cmd.Parameters.AddWithValue("osm_version", delete.Version.Value);

                        cmd.ExecuteNonQuery();
                    }
                }

                if (osmChange.Modify != null)
                {
                    foreach (var modify in osmChange.Modify)
                    {
                        cmd = this.GetCommand(
                            "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                               "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                        cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
                        cmd.Parameters.AddWithValue("type", Constants.ModifyType);
                        cmd.Parameters.AddWithValue("osm_id", modify.Id.Value);
                        cmd.Parameters.AddWithValue("osm_type", (int)modify.Type);
                        cmd.Parameters.AddWithValue("osm_version", modify.Version.Value);

                        cmd.ExecuteNonQuery();
                    }
                }
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
        /// Gets an osm object of the given type and the given id and version.
        /// </summary>
        public OsmGeo Get(OsmGeoType type, long id, int version)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    return this.GetNode(id, version);
                case OsmGeoType.Way:
                    return this.GetWay(id, version);
                case OsmGeoType.Relation:
                    return this.GetRelation(id, version);
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
        /// Gets all osm objects with the given types and the given id's and versions.
        /// </summary>
        public IList<OsmGeo> Get(OsmGeoType type, IList<long> id, IList<int> versions)
        {
            if (id == null) { throw new ArgumentNullException("id"); }

            var result = new List<OsmGeo>();
            for (int i = 0; i < id.Count; i++)
            {
                result.Add(this.Get(type, id[i], versions[i]));
            }
            return result;
        }

        /// <summary>
        /// Disposes of all resources associated with this db.
        /// </summary>
        public void Dispose()
        {
            _connection.Dispose();
        }

        /// <summary>
        /// Hides the given object.
        /// </summary>
        private bool Hide(OsmGeo osmGeo)
        {
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    return this.HideNode(osmGeo as Node);
                case OsmGeoType.Way:
                    return this.HideWay(osmGeo as Way);
                case OsmGeoType.Relation:
                    return this.HideRelation(osmGeo as Relation);
            }
            return false;
        }

        /// <summary>
        /// Adds or updates a node.
        /// </summary>
        private bool AddOrUpdateNode(Node node)
        {
            var wasUpdate = true;
            var cmd = this.GetCommand("delete from node_tags where node_id = @node_id and node_version = @version");
            cmd.Parameters.AddWithValue("node_id", node.Id.Value);
            cmd.Parameters.AddWithValue("version", node.Version.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("update node set latitude=@latitude, longitude=@longitude, changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, tile=@tile, usr=@usr, usr_id=@usr_id " +
                "where id=@id and version=@version");
            cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
            cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
            cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", node.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("tile", Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id);
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
                cmd.Parameters.AddWithValue("tile", Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id);
                cmd.Parameters.AddWithValue("version", node.Version.Value);
                cmd.Parameters.AddWithValue("usr", node.UserName);
                cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (node.Tags != null)
            {
                cmd = this.GetCommand("INSERT into node_tags (node_id, node_version, [key], value) VALUES (@node_id, @node_version, @key, @value)");
                cmd.Parameters.AddWithValue("node_id", node.Id.Value);
                cmd.Parameters.AddWithValue("node_version", node.Version.Value);
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
        /// Sets the given node to invisible.
        /// </summary>
        private bool HideNode(Node node)
        {
            var cmd = this.GetCommand("update node set visible=0 where id=@id and version=@version");
            cmd.Parameters.AddWithValue("id", node.Id.Value);
            cmd.Parameters.AddWithValue("version", node.Version.Value);

            return (cmd.ExecuteNonQuery() != 0);
        }

        /// <summary>
        /// Adds or updates a way.
        /// </summary>
        private bool AddOrUpdateWay(Way way)
        {
            var wasUpdate = true;
            var cmd = this.GetCommand("delete from way_tags where way_id=@way_id and way_version=@way_version");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.Parameters.AddWithValue("way_version", way.Version.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from way_nodes where way_id = @way_id and way_version=@way_version");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.Parameters.AddWithValue("way_version", way.Version.Value);
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
                cmd = this.GetCommand("INSERT into way_tags (way_id, way_version, [key], value) VALUES (@way_id, @way_version, @key, @value)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.AddWithValue("way_version", way.Version.Value);
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
                cmd = this.GetCommand("INSERT into way_nodes (way_id, way_version, node_id, sequence_id) VALUES (@way_id, @way_version, @node_id, @sequence_id)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.AddWithValue("way_version", way.Version.Value);
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
        /// Sets the given way to invisible.
        /// </summary>
        private bool HideWay(Way way)
        {
            var cmd = this.GetCommand("update way set visible=0 where id=@id and version=@version");
            cmd.Parameters.AddWithValue("version", way.Version.Value);
            cmd.Parameters.AddWithValue("id", way.Id.Value);

            return (cmd.ExecuteNonQuery() != 0);
        }

        /// <summary>
        /// Adds or updates a relation.
        /// </summary>
        private bool AddOrUpdateRelation(Relation relation)
        {
            var wasUpdate = true;
            var cmd = this.GetCommand("delete from relation_tags where relation_id = @relation_id and relation_version=@relation_version");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.Parameters.AddWithValue("relation_version", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from relation_members where relation_id = @relation_id and relation_version=@relation_version");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.Parameters.AddWithValue("relation_version", relation.Id.Value);
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
                cmd = this.GetCommand("INSERT into relation_tags (relation_id, relation_version, [key], value) VALUES (@relation_id, @relation_version, @key, @value)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.AddWithValue("relation_version", relation.Version.Value);
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
                cmd = this.GetCommand("INSERT into relation_members (relation_id, relation_version, member_id, member_type, member_role, sequence_id) VALUES (@relation_id, @relation_version, @member_id, @member_type, @member_role, @sequence_id)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.AddWithValue("relation_version", relation.Version.Value);
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
        /// Sets the given relation to invisible.
        /// </summary>
        private bool HideRelation(Relation relation)
        {
            var cmd = this.GetCommand("update relation set visible=0 where id=@id and version=@version");
            cmd.Parameters.AddWithValue("version", relation.Version.Value);
            cmd.Parameters.AddWithValue("id", relation.Id.Value);

            return (cmd.ExecuteNonQuery() != 0);
        }

        /// <summary>
        /// Gets the latest visible version of the node with the given id.
        /// </summary>
        private Node GetNode(long id)
        {
            var command = this.GetCommand("SELECT max(version) FROM node WHERE id = @id and visible=1");
            command.Parameters.AddWithValue("id", id);
            var version = command.ExecuteScalarLong(0);
            if (version == 0)
            { // no data found.
                return null;
            }
            return this.GetNode(id, (int)version);
        }

        /// <summary>
        /// Gets the node with the given id and version.
        /// </summary>
        private Node GetNode(long id, int version)
        {
            var command = this.GetCommand("SELECT * FROM node WHERE id = @id and version = @version");
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("version", version);

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
                command = this.GetCommand("SELECT * FROM node_tags WHERE node_id = @id and node_version = @node_version");
                command.Parameters.AddWithValue("id", id);
                command.Parameters.AddWithValue("node_version", version);

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
        /// Gets the latest visible version of way node with the given id.
        /// </summary>
        private Way GetWay(long id)
        {
            var command = this.GetCommand("SELECT max(version) FROM way WHERE id = @id and visible=1");
            command.Parameters.AddWithValue("id", id);
            var version = command.ExecuteScalarLong(0);
            if (version == 0)
            { // no data found.
                return null;
            }
            return this.GetWay(id, (int)version);
        }

        /// <summary>
        /// Gets the way with the given id.
        /// </summary>
        private Way GetWay(long id, int version)
        {
            var command = this.GetCommand("SELECT * FROM way WHERE id = @id and version = @version");
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("version", version);

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
                command = this.GetCommand("SELECT * FROM way_nodes WHERE way_id = @id and way_version=@way_version ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);
                command.Parameters.AddWithValue("way_version", version);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddNodes(way);
                    }
                }

                command = this.GetCommand("SELECT * FROM way_tags WHERE way_id = @id and way_version=@way_version");
                command.Parameters.AddWithValue("id", id);
                command.Parameters.AddWithValue("way_version", version);

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
        /// Gets the latest visible version of the relation with the given id.
        /// </summary>
        private Relation GetRelation(long id)
        {
            var command = this.GetCommand("SELECT max(version) FROM relation WHERE id = @id and visible=1");
            command.Parameters.AddWithValue("id", id);
            var version = command.ExecuteScalarLong(0);
            if (version == 0)
            { // no data found.
                return null;
            }
            return this.GetRelation(id, (int)version);
        }

        /// <summary>
        /// Gets the relation with the given id and version.
        /// </summary>
        private Relation GetRelation(long id, int version)
        {
            var command = this.GetCommand("SELECT * FROM relation WHERE id = @id and version = @version");
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("version", version);

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
                command = this.GetCommand("SELECT * FROM relation_members WHERE relation_id=@id and relation_version=@relation_version ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);
                command.Parameters.AddWithValue("relation_version", version);

                using (var reader = command.ExecuteReaderWrapper())
                {
                    if (reader.Read())
                    {
                        reader.AddMembers(relation);
                    }
                }

                command = this.GetCommand("SELECT * FROM relation_tags WHERE relation_id=@id and relation_version=@relation_version");
                command.Parameters.AddWithValue("id", id);
                command.Parameters.AddWithValue("relation_version", version);

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

        /// <summary>
        /// Opens a new changeset.
        /// </summary>
        public long OpenChangeset(Changeset info)
        {
            var highestChangesetId = this.GetCommand("select max(id) from changeset").ExecuteScalarLong(0);
            highestChangesetId++;

            info.Id = highestChangesetId;

            var cmd = this.GetCommand(
                "update changeset set " +
                "closed_at=@closed_at, created_at=@created_at, " +
                "min_lat=@min_lat, min_lon=@min_lon, max_lat=@max_lat, max_lon=@max_lon, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", info.Id.Value);
            cmd.Parameters.AddWithValue("closed_at", info.ClosedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("created_at", info.CreatedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("min_lat", info.MinLatitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("min_lon", info.MinLongitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("max_lat", info.MaxLatitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("max_lon", info.MaxLongitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("usr_id", info.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = this.GetCommand(
                    "INSERT INTO changeset (id, closed_at, created_at, min_lat, min_lon, max_lat, max_lon, usr_id) " +
                       "VALUES(@id, @closed_at, @created_at, @min_lat, @min_lon, @max_lat, @max_lon, @usr_id)");
                cmd.Parameters.AddWithValue("id", info.Id.Value);
                cmd.Parameters.AddWithValue("closed_at", info.ClosedAt.ToUnixTimeDB());
                cmd.Parameters.AddWithValue("created_at", info.CreatedAt.ToUnixTimeDB());
                cmd.Parameters.AddWithValue("min_lat", info.MinLatitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("min_lon", info.MinLongitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("max_lat", info.MaxLatitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("max_lon", info.MaxLongitude.ConvertToDBValue());
                cmd.Parameters.AddWithValue("usr_id", info.UserId.Value);

                cmd.ExecuteNonQuery();
            }

            if (info.Tags != null)
            {
                cmd = this.GetCommand("INSERT into changeset_tags (changeset_id, [key], value) VALUES (@changeset_id, @key, @value)");
                cmd.Parameters.AddWithValue("changeset_id", info.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in info.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            return highestChangesetId;
        }

        /// <summary>
        /// Applies the given changeset.
        /// </summary>
        public DiffResultResult ApplyChangeset(long id, OsmChange changeset, bool bestEffort = true)
        {
            var cmd = this.GetCommand("select count(*) from changeset where id=@id and closed_at is null");
            cmd.Parameters.AddWithValue("id", id);
            if (cmd.ExecuteScalarLong() <= 0)
            {
                return new DiffResultResult("Changeset not open");
            }

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
                            if (!this.AddOrUpdateNode(modify as Node))
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
                            if (!this.AddOrUpdateWay(modify as Way))
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
                            if (!this.AddOrUpdateRelation(modify as Relation))
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
                    if (this.Hide(delete))
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

            // save changes.

            if (changeset.Create != null)
            {
                foreach (var create in changeset.Create)
                {
                    cmd = this.GetCommand(
                        "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                           "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                    cmd.Parameters.AddWithValue("changeset_id", id);
                    cmd.Parameters.AddWithValue("type", Constants.CreateType);
                    cmd.Parameters.AddWithValue("osm_id", create.Id.Value);
                    cmd.Parameters.AddWithValue("osm_type", (int)create.Type);
                    cmd.Parameters.AddWithValue("osm_version", create.Version.Value);

                    cmd.ExecuteNonQuery();
                }
            }

            if (changeset.Delete != null)
            {
                foreach (var delete in changeset.Delete)
                {
                    cmd = this.GetCommand(
                        "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                           "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                    cmd.Parameters.AddWithValue("changeset_id", id);
                    cmd.Parameters.AddWithValue("type", Constants.DeleteType);
                    cmd.Parameters.AddWithValue("osm_id", delete.Id.Value);
                    cmd.Parameters.AddWithValue("osm_type", (int)delete.Type);
                    cmd.Parameters.AddWithValue("osm_version", delete.Version.Value);

                    cmd.ExecuteNonQuery();
                }
            }

            if (changeset.Modify != null)
            {
                foreach (var modify in changeset.Modify)
                {
                    cmd = this.GetCommand(
                        "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                           "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                    cmd.Parameters.AddWithValue("changeset_id", id);
                    cmd.Parameters.AddWithValue("type", Constants.ModifyType);
                    cmd.Parameters.AddWithValue("osm_id", modify.Id.Value);
                    cmd.Parameters.AddWithValue("osm_type", (int)modify.Type);
                    cmd.Parameters.AddWithValue("osm_version", modify.Version.Value);

                    cmd.ExecuteNonQuery();
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
        /// Updates the changeset info.
        /// </summary>
        public bool UpdateChangesetInfo(Changeset changeset)
        {
            var cmd = this.GetCommand("delete from changeset_tags where changeset_id = @changeset_id");
            cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from changeset_changes where changeset_id = @changeset_id");
            cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "update changeset set created_at=@created_at, " +
                "min_lat=@min_lat, min_lon=@min_lon, max_lat=@max_lat, max_lon=@max_lon, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", changeset.Id.Value);
            cmd.Parameters.AddWithValue("created_at", changeset.CreatedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("min_lat", changeset.MinLatitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("min_lon", changeset.MinLongitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("max_lat", changeset.MaxLatitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("max_lon", changeset.MaxLongitude.ConvertToDBValue());
            cmd.Parameters.AddWithValue("usr_id", changeset.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                return false;
            }

            if (changeset.Tags != null)
            {
                cmd = this.GetCommand("INSERT into changeset_tags (changeset_id, [key], value) VALUES (@changeset_id, @key, @value)");
                cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in changeset.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }
            return true;
        }

        /// <summary>
        /// Closes the given changeset.
        /// </summary>
        public bool CloseChangeset(long id)
        {
            var cmd = this.GetCommand(
                "update changeset set closed_at=@closed_at where id=@id");
            cmd.Parameters.AddWithValue("id", id);
            cmd.Parameters.AddWithValue("closed_at", DateTime.Now.ToUnixTime());

            return (cmd.ExecuteNonQuery() != 0);
        }
    }
}