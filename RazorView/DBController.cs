/*
Copyright 2012, 2013 Gnoso Inc.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except for what is in compliance with the License.

You may obtain a copy of this license at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.

See the License for the specific language governing permissions and limitations.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using RazorDB;
using System.Text.RegularExpressions;

namespace RazorView {

    public class ByteViz : IDataViz {

        public string TransformKey(byte[] key) {
            return key.ToHexString();
        }

        public string TransformValue(byte[] value) {
            return value.ToHexString();
        }
    }

    public class Record {
        public Record(int index, KeyValuePair<byte[], byte[]> pair, IDataViz viz) {
            Index = index;
            key = pair.Key;
            value = pair.Value;
            _viz = viz;
        }
        private IDataViz _viz;

        public int Index { get; set; }

        private byte[] key;
        public string Key {
            get { return _viz.TransformKey(key); }
        }
        private byte[] value;
        public string Value {
            get { return _viz.TransformValue(value); }
        }
    }

    public class DBController {

        public DBController(string journalFile, IEnumerable<IDataVizFactory> vizFactories) {
            string path = Path.GetDirectoryName(journalFile);
            _db = new KeyValueStore(path);

            foreach (var vf in vizFactories) {
                var v = vf.GetVisualizer(_db);
                if (v != null) {
                    _viz = v;
                    break;
                }
            }
            if (_viz == null)
                _viz = new ByteViz();
        }

        private KeyValueStore _db;
        private IDataViz _viz;

        public IEnumerable<Record> GetRecords(string keyFilter, string valueFilter) {
            int index = 0;
            var collection = _db.Enumerate().Select(pair => new Record(index++, pair, _viz)); 
            if (!string.IsNullOrWhiteSpace(keyFilter)) {
                Regex reg = new Regex(keyFilter, RegexOptions.None);
                collection = collection.Where(rec => reg.IsMatch(rec.Key));
            }
            if (!string.IsNullOrWhiteSpace(valueFilter)) {
                Regex reg = new Regex(valueFilter, RegexOptions.None);
                collection = collection.Where(rec => reg.IsMatch(rec.Value));
            }
            return collection;
        }

        public void Close() {
            _db.Close();
        }

        public string GetAnalysisText() {
            int recordCount = 0;
            int keySize = 0;
            int valueSize = 0;
            foreach (var pair in _db.Enumerate()) {
                recordCount++;
                keySize += pair.Key.Length;
                valueSize += pair.Value.Length;
            }
            return string.Format("Total Records: {0}\nKey Size: {1} MB\nValue Size: {2} MB\n", recordCount, (double)keySize/1024/1024, (double)valueSize/1024/1024);
        }
    }
}
