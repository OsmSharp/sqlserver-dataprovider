// OsmSharp - OpenStreetMap (OSM) SDK
//
// Copyright (C) 2016 Abelshausen Ben
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

using OsmSharp.Streams;
using System;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Streams
{
    /// <summary>
    /// An osm stream source that reads all data from the database. 
    /// </summary>
    public class HistoryDbStreamSource : OsmStreamSource
    {
        private readonly string _connectionString;

        /// <summary>
        /// Creates a new history db.
        /// </summary>
        public HistoryDbStreamSource(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Creates a new history db.
        /// </summary>
        public HistoryDbStreamSource(SqlConnection connection)
        {
            _connection = connection;
        }

        private SqlConnection _connection; // Holds the connection to the SQLServer db.

        private DbDataReaderWrapper _nodeReader;
        private DbDataReaderWrapper _nodeTagsReader;
        private DbDataReaderWrapper _wayReader;
        private DbDataReaderWrapper _wayTagsReader;
        private DbDataReaderWrapper _wayNodesReader;
        private DbDataReaderWrapper _relationReader;
        private DbDataReaderWrapper _relationMembersReader;
        private DbDataReaderWrapper _relationTagsReader;

        private OsmGeoType? _currentType;

        /// <summary>
        /// Gets the connection.
        /// </summary>
        private SqlConnection GetConnection()
        {
            if (_connection == null)
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
        /// Returns true if this source can be reset.
        /// </summary>
        public override bool CanReset
        {
            get
            {
                return true;
            }
        }

        /// <summary>
        /// Initializes this source.
        /// </summary>
        public override void Initialize()
        {
            var command = this.GetCommand("SELECT id, latitude, longitude, changeset_id, visible, timestamp, tile, [version], usr, usr_id " +
                "FROM dbo.node " +
                "ORDER BY id, [version]");
            _nodeReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT node_id, [version], [key], value " +
                "FROM dbo.node_tags " +
                "ORDER BY node_id, [version]");
            _nodeTagsReader = new DbDataReaderWrapper(command.ExecuteReader());

            command = this.GetCommand("SELECT id, changeset_id, visible, timestamp, [version], usr, usr_id " +
                "FROM dbo.way " +
                "ORDER BY id, [version]");
            _wayReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT way_id, [version], [key], value " +
                "FROM dbo.way_tags " +
                "ORDER BY way_id, [version]");
            _wayTagsReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT way_id, [version], node_id, sequence_id  " +
                "FROM dbo.way_nodes " +
                "ORDER BY way_id, [version], sequence_id");
            _wayNodesReader = new DbDataReaderWrapper(command.ExecuteReader());

            command = this.GetCommand("SELECT id, changeset_id, visible, timestamp, [version], usr, usr_id " +
                "FROM dbo.relation " +
                "ORDER BY id, [version]");
            _relationReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT relation_id, [version], [key], value " +
                "FROM dbo.relation_tags " +
                "ORDER BY relation_id, [version]");
            _relationTagsReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT relation_id, [version], member_type, member_role, member_id, sequence_id " +
                "FROM dbo.relation_members " +
                "ORDER BY relation_id, [version], sequence_id");
            _relationMembersReader = new DbDataReaderWrapper(command.ExecuteReader());
        }

        /// <summary>
        /// Returns the current object.
        /// </summary>
        /// <returns></returns>
        public override OsmGeo Current()
        {
            if(_currentType == null)
            {
                throw new Exception("No current object available.");
            }

            if(_currentType.Value == OsmGeoType.Node)
            {
                var node = _nodeReader.BuildNode();
                _nodeTagsReader.AddTags(node);
                return node;
            }
            else if (_currentType.Value == OsmGeoType.Way)
            {
                var way = _wayReader.BuildWay();
                _wayTagsReader.AddTags(way);
                _wayNodesReader.AddNodes(way);
                return way;
            }
            else if (_currentType.Value == OsmGeoType.Relation)
            {
                var relation = _relationReader.BuildRelation();
                _relationTagsReader.AddTags(relation);
                _relationMembersReader.AddMembers(relation);
                return relation;
            }
            throw new Exception("No current object available.");
        }

        /// <summary>
        /// Move to the next object.
        /// </summary>
        public override bool MoveNext(bool ignoreNodes, bool ignoreWays, bool ignoreRelations)
        {
            if(_currentType == null)
            { // first move.
                _currentType = OsmGeoType.Node;
                _nodeTagsReader.Read();
            }

            if(_currentType.Value == OsmGeoType.Node)
            {
                if(ignoreNodes)
                { // ignore nodes, move to way..
                    _currentType = OsmGeoType.Way;
                    _wayNodesReader.Read();
                    _wayTagsReader.Read();
                }
                else
                { // nodes not be be ignored, move to next node.
                    if(_nodeReader.Read())
                    { // move succeeded.
                        return true;
                    }
                    else
                    { // move not succeeded, move to ways.
                        _currentType = OsmGeoType.Way;
                        _wayNodesReader.Read();
                        _wayTagsReader.Read();
                    }
                }
            }
            if (_currentType.Value == OsmGeoType.Way)
            {
                if (ignoreWays)
                { // ignore ways, move to relations..
                    _currentType = OsmGeoType.Relation;
                    _relationMembersReader.Read();
                    _relationTagsReader.Read();
                }
                else
                { // ways not be ignored, move to next way.
                    if(_wayReader.Read())
                    { // move succeeded.
                        return true;
                    }
                    else
                    { // move no success, move to relations.
                        _currentType = OsmGeoType.Relation;
                        _relationMembersReader.Read();
                        _relationTagsReader.Read();
                    }
                }
            }
            if (_currentType.Value == OsmGeoType.Relation)
            {
                if(ignoreRelations)
                { // ignore relations.
                    return false;
                }
                else
                { // don't ignore relations, try to read.
                    if(_relationReader.Read())
                    {
                        return true;
                    }
                    else
                    { // move no success.
                        return false;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Resets this source.
        /// </summary>
        public override void Reset()
        {
            if (_nodeReader != null) { (_nodeReader as IDisposable).Dispose(); }
            if (_nodeTagsReader != null) { (_nodeTagsReader as IDisposable).Dispose(); }
            if (_wayReader != null) { (_wayReader as IDisposable).Dispose(); }
            if (_wayTagsReader != null) { (_wayTagsReader as IDisposable).Dispose(); }
            if (_wayNodesReader != null) { (_wayNodesReader as IDisposable).Dispose(); }
            if (_relationReader != null) { (_relationReader as IDisposable).Dispose(); }
            if (_relationMembersReader != null) { (_relationMembersReader as IDisposable).Dispose(); }
            if (_relationTagsReader != null) { (_relationTagsReader as IDisposable).Dispose(); }

            this.Initialize();
        }
    }
}