// OsmSharp - OpenStreetMap (OSM) SDK
//
// Copyright (C) 2016 Abelshausen Ben
//                    Alexander Sinitsyn
// 
// This file is part of OsmSharp.
// 
// OsmSharp is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// OsmSharp is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with OsmSharp. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using OsmSharp.Osm.Data;
using OsmSharp.Data.SQLServer.Osm.SchemaTools;
using OsmSharp.Math.Geo;
using OsmSharp.Osm;
using OsmSharp.Osm.Filters;
using OsmSharp.Collections.Tags;
using System.Data;
using OsmSharp.Osm.Tiles;
using OsmSharp.Osm.Streams;
using System.Text;

namespace OsmSharp.Data.SQLServer.Osm
{
    /// <summary>
    /// Implements a snapshot db storing a snapshot of OSM-data in an SQL-server database.
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
            if(_connection == null)
            {
                _connection = new SqlConnection(_connectionString);
                _connection.Open();
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
        /// <returns></returns>
        public OsmStreamSource Get()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns all the objects within a given bounding box and filtered by a given filter.
        /// </summary>
        public IList<OsmGeo> Get(float minLatitude, float minLongitude, float maxLatitude, float maxLongitude, Filter filter)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Clears all data.
        /// </summary>
        public void Clear()
        {
            SchemaTools.SchemaTools.SnapshotDbDeleteAllData(this.GetConnection());
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
                this.AddOrUpdate(osmGeos);
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
        public IList<OsmGeo> Get(IList<OsmGeoType> type, IList<long> id)
        {
            if (type == null) { throw new ArgumentNullException("type"); }
            if (id == null) { throw new ArgumentNullException("id"); }
            if (id.Count != type.Count) { throw new ArgumentException("Type and id lists need to have the same size."); }

            var result = new List<OsmGeo>();
            for (int i = 0; i < id.Count; i++)
            {
                result.Add(this.Get(type[i], id[i]));
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
                        string.Format("DELETE FROM node_tag WHERE (node_id IN ({0})", 
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM node WHERE (id IN ({0})",
                            id.ToInvariantString()));
                    break;
                case OsmGeoType.Way:
                    command = this.GetCommand(
                        string.Format("DELETE FROM way_tags WHERE (way_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM way_nodes WHERE (way_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM way WHERE (id IN ({0})",
                            id.ToInvariantString()));
                    break;
                case OsmGeoType.Relation:
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation_tags WHERE (relation_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation_members WHERE (relation_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation WHERE (id IN ({0})",
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
        /// <param name="atomic">Then true, it's the entire changeset or nothing. When false the changeset is applied using best-effort.</param>
        /// <returns>True when the entire changeset was applied without issues, false otherwise.</returns>
        public bool ApplyChangeset(ChangeSet changeset, bool atomic = false)
        {
            throw new NotImplementedException();
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
        /// <param name="node"></param>
        private void AddOrUpdateNode(Node node)
        {
            var cmd = this.GetCommand("DELETE node_tags where node_id = @node_id");
            cmd.Parameters.AddWithValue("node_id", node.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "UPDATE node SET " +
                "latitude=@latitude, longitude=@longitude, changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, tile=@tile, " +
                "version=@version, usr=@usr, usr_id=@usr_id, " +
                "WHERE id=@id");
            cmd.Parameters.AddWithValue("id", node.Id.Value);
            cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
            cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
            cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", node.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value);
            cmd.Parameters.AddWithValue("tile", Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value,
                TileDefaultsForRouting.Zoom).Id);
            cmd.Parameters.AddWithValue("version", node.Version.Value);
            cmd.Parameters.AddWithValue("usr", node.UserName);
            cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            { // oeps, node did not exist, insert.
                cmd = this.GetCommand(
                    "INSERT INTO node (id,latitude,longitude,changeset_id,visible,timestamp,tile,version,usr,usr_id) " +
                    "VALUES(@id,@latitude,@longitude,@changeset_id,@visible,@timestamp,@tile,version,@usr,@usr_id)");
                cmd.Parameters.AddWithValue("id", node.Id.Value);
                cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
                cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
                cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", node.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value);
                cmd.Parameters.AddWithValue("tile", Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value,
                    TileDefaultsForRouting.Zoom).Id);
                cmd.Parameters.AddWithValue("version", node.Version.Value);
                cmd.Parameters.AddWithValue("usr", node.UserName);
                cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);
            }

            cmd = this.GetCommand(string.Empty);
            var cmdText = new StringBuilder("INSERT into node_tags (node_id,key,value) VALUES ");
            if (node.Tags != null)
            {
                cmd.Parameters.AddWithValue("node_id", node.Id.Value);
                var t = 0;
                foreach (var tag in node.Tags)
                {
                    cmdText.Append(string.Format("(@node_id, @key_id_{0}, @value_id_{0})", t));
                    cmd.Parameters.AddWithValue(string.Format("key_id_{0}", t), tag.Key);
                    cmd.Parameters.AddWithValue(string.Format("value_id_{0}", t), tag.Value);
                    t++;
                }
                if (t > 0)
                {
                    cmd.CommandText = cmdText.ToInvariantString();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds or updates a way.
        /// </summary>
        private void AddOrUpdateWay(Way way)
        {
            var cmd = this.GetCommand("DELETE way_tags where way_id = @way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("DELETE way_nodes where way_id = @way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "UPDATE way SET " +
                "changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, " +
                "version=@version, usr=@usr, usr_id=@usr_id, " +
                "WHERE id=@id");
            cmd.Parameters.AddWithValue("id", way.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", way.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value);
            cmd.Parameters.AddWithValue("version", way.Version.Value);
            cmd.Parameters.AddWithValue("usr", way.UserName);
            cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            { // oeps, node did not exist, insert.
                cmd = this.GetCommand(
                    "INSERT INTO way (id,changeset_id,visible,timestamp,version,usr,usr_id) " +
                    "VALUES(@id,@changeset_id,@visible,@timestamp,version,@usr,@usr_id)");
                cmd.Parameters.AddWithValue("id", way.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", way.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value);
                cmd.Parameters.AddWithValue("version", way.Version.Value);
                cmd.Parameters.AddWithValue("usr", way.UserName);
                cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);
            }

            cmd = this.GetCommand(string.Empty);
            var cmdText = new StringBuilder("INSERT into way_tags (way_id,key,value) VALUES ");
            if (way.Tags != null)
            {
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                var t = 0;
                foreach (var tag in way.Tags)
                {
                    cmdText.Append(string.Format("(@way_id, @key_id_{0}, @value_id_{0})", t));
                    cmd.Parameters.AddWithValue(string.Format("key_id_{0}", t), tag.Key);
                    cmd.Parameters.AddWithValue(string.Format("value_id_{0}", t), tag.Value);
                    t++;
                }
                if (t > 0)
                {
                    cmd.CommandText = cmdText.ToInvariantString();
                    cmd.ExecuteNonQuery();
                }
            }

            cmd = this.GetCommand(string.Empty);
            cmdText = new StringBuilder("INSERT into way_nodes (way_id,node_id,sequence_id) VALUES ");
            if (way.Nodes != null)
            {
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                for (var n = 0; n < way.Nodes.Count; n++)
                {
                    cmdText.Append(string.Format("(@way_id, @node_id{0}, @sequence_id{0})", n));
                    cmd.Parameters.AddWithValue(string.Format("node_id{0}", n), way.Nodes[n]);
                    cmd.Parameters.AddWithValue(string.Format("sequence_id{0}", n), n);
                }
                if (way.Nodes.Count > 0)
                {
                    cmd.CommandText = cmdText.ToInvariantString();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds or updates a relation.
        /// </summary>
        private void AddOrUpdateRelation(Relation relation)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the node with the given id.
        /// </summary>
        private Node GetNode(long id)
        {
            var command = this.GetCommand("SELECT id, latitude, longitude, changeset_id, visible, timestamp, tile, version, usr, usr_id FROM dbo.node WHERE id = @id");
            command.Parameters.AddWithValue("id", id);

            Node node = null;
            using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
            {
                if (reader.Read())
                {
                    node = reader.BuildNode();
                }
            }
            if (node != null)
            {
                command = this.GetCommand("SELECT node_id, key, value FROM dbo.node_tags WHERE way_id = @id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
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
            var command = this.GetCommand("SELECT id, changeset_id, visible, timestamp, version, usr, usr_id FROM dbo.way WHERE id = @id");
            command.Parameters.AddWithValue("id", id);

            Way way = null;
            using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
            {
                if (reader.Read())
                {
                    way = reader.BuildWay();
                }
            }
            if(way != null)
            {
                command = this.GetCommand("SELECT way_id, node_id, sequence_id FROM dbo.way_nodes WHERE way_id = @id ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddNodes(way);
                    }
                }

                command = this.GetCommand("SELECT way_id, key, value FROM dbo.way_tags WHERE way_id = @id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
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
            var command = this.GetCommand("SELECT id, changeset_id, visible, timestamp, version, usr, usr_id FROM dbo.relation WHERE id = @id");
            command.Parameters.AddWithValue("id", id);

            Relation relation = null;
            using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
            {
                if (reader.Read())
                {
                    relation = reader.BuildRelation();
                }
            }
            if (relation != null)
            {
                command = this.GetCommand("SELECT relation_id, member_type, member_role, sequence_id FROM dbo.relation_members WHERE relation_id = @id ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddMembers(relation);
                    }
                }

                command = this.GetCommand("SELECT way_id, key, value FROM dbo.relation_tags WHERE way_id = @id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
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
