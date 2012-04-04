using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using RazorDB;

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

        public IEnumerable<Record> Records {
            get {
                int index = 0;
                return _db.Enumerate().Select( pair => new Record(index++, pair, _viz) ); 
            }
        }

        public void Close() {
            _db.Close();
        }
    }
}
