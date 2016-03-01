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
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Enumerators
{
    class RelationEnumerable : IEnumerable<Relation>
    {
        private readonly SqlCommand _relationCommand;
        private readonly SqlCommand _relationTagsCommand;
        private readonly SqlCommand _relationMembersCommand;

        public RelationEnumerable(SqlCommand relationCommand,
            SqlCommand relationTagsCommand, SqlCommand relationMembersCommand)
        {
            _relationCommand = relationCommand;
            _relationTagsCommand = relationTagsCommand;
            _relationMembersCommand = relationMembersCommand;
        }

        public IEnumerator<Relation> GetEnumerator()
        {
            return new RelationEnumerator(_relationCommand, _relationTagsCommand, _relationMembersCommand);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new RelationEnumerator(_relationCommand, _relationTagsCommand, _relationMembersCommand);
        }
    }

    class RelationEnumerator : IEnumerator<Relation>
    {
        private readonly SqlCommand _relationCommand;
        private readonly SqlCommand _relationTagsCommand;
        private readonly SqlCommand _relationMembersCommand;

        public RelationEnumerator(SqlCommand relationCommand,
            SqlCommand relationTagsCommand, SqlCommand relationMembersCommand)
        {
            _relationCommand = relationCommand;
            _relationTagsCommand = relationTagsCommand;
            _relationMembersCommand = relationMembersCommand;
        }

        private Relation _current;
        private DbDataReaderWrapper _relationReader;
        private DbDataReaderWrapper _relationTagsReader;
        private DbDataReaderWrapper _relationMembersReader;

        public Relation Current
        {
            get
            {
                if (_current == null)
                {
                    throw new InvalidOperationException("No data available.");
                }
                return _current;
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return this.Current;
            }
        }

        public void Dispose()
        {

        }

        public bool MoveNext()
        {
            if (_relationReader == null)
            {
                _relationReader = _relationCommand.ExecuteReaderWrapper();
                if (_relationTagsCommand != null)
                {
                    _relationTagsReader = _relationTagsCommand.ExecuteReaderWrapper();
                    _relationTagsReader.Read();
                }
                if (_relationMembersCommand != null)
                {
                    _relationMembersReader = _relationMembersCommand.ExecuteReaderWrapper();
                    _relationMembersReader.Read();
                }
            }

            if (!_relationReader.Read())
            {
                return false;
            }

            if (!_relationReader.HasActiveRow)
            {
                return false;
            }

            var relation = new Relation();
            _relationReader.BuildRelation(relation);
            if (_relationTagsReader != null)
            {
                _relationTagsReader.AddTags(relation);
            }
            if (_relationMembersReader != null)
            {
                _relationMembersReader.AddMembers(relation);
            }
            _current = relation;
            return true;
        }

        public void Reset()
        {
            if (_relationReader != null)
            {
                _relationReader.Dispose();
            }
            if (_relationTagsReader != null)
            {
                _relationTagsReader.Dispose();
            }
            if (_relationMembersReader != null)
            {
                _relationMembersReader.Dispose();
            }

            _relationReader = null;
            _relationTagsReader = null;
            _relationMembersReader = null;
        }
    }
}