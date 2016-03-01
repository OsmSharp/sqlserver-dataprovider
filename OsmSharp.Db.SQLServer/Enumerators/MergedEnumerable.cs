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

using System.Collections;
using System.Collections.Generic;

namespace OsmSharp.Db.SQLServer.Enumerators
{
    class MergedEnumerable : IEnumerable<OsmGeo>
    {
        private IEnumerable<OsmGeo>[] _enumerables;

        public MergedEnumerable(params IEnumerable<OsmGeo>[] enumerables)
        {
            _enumerables = enumerables;
        }

        public IEnumerator<OsmGeo> GetEnumerator()
        {
            return new MergedEnumerator(_enumerables);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new MergedEnumerator(_enumerables);
        }
    }
    class MergedEnumerator : IEnumerator<OsmGeo>
    {
        private readonly IEnumerable<OsmGeo>[] _enumerables;

        public MergedEnumerator(IEnumerable<OsmGeo>[] enumerables)
        {
            _enumerables = enumerables;
        }

        private IEnumerator<OsmGeo>[] _enumerators;
        private OsmGeo _current;

        public OsmGeo Current
        {
            get
            {
                return _current;
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return _current;
            }
        }

        public void Dispose()
        {

        }

        public bool MoveNext()
        {
            if (_enumerators == null)
            {
                _enumerators = new IEnumerator<OsmGeo>[_enumerables.Length];

                for(var i = 0; i < _enumerables.Length; i++)
                {
                    _enumerators[i] = _enumerables[i].GetEnumerator();
                    if (!_enumerators[i].MoveNext())
                    {
                        _enumerators[i] = null;
                    }
                }
            }

            // determine the 'lowest' object.
            _current = null;
            var e = -1;
            for (var i = 0; i < _enumerators.Length; i++)
            {
                if (_enumerators[i] != null)
                {
                    if (_current == null)
                    {
                        _current = _enumerators[i].Current;
                        e = i;
                    }
                    else
                    {
                        var current = _enumerators[i].Current;
                        if (current.Type == _current.Type)
                        {
                            if (current.Id.Value == _current.Id.Value &&
                                current.Version.Value < _current.Version.Value)
                            {
                                _current = current;
                                e = i;
                            }
                            else if (current.Id.Value < _current.Id.Value)
                            {
                                _current = current;
                                e = i;
                            }
                        }
                        else if (current.Type == OsmGeoType.Node)
                        {
                            _current = current;
                            e = i;
                        }
                        else if (current.Type == OsmGeoType.Way &&
                            _current.Type == OsmGeoType.Relation)
                        {
                            _current = current;
                            e = i;
                        }
                    }
                }
            }
            if (e != -1)
            {
                if (!_enumerators[e].MoveNext())
                {
                    _enumerators[e] = null;
                }
            }
            return _current != null;
        }

        public void Reset()
        {
            _enumerators = null;
        }
    }
}
