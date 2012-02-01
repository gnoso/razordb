using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RazorDB {

    public class JournalWriter {

        public JournalWriter(string baseFileName, int version) {
            _fileName = Config.JournalFile(baseFileName, version);
            _writer = new BinaryWriter(new FileStream(_fileName, FileMode.Create, FileAccess.Write, FileShare.None, 1024, false));
        }

        private BinaryWriter _writer;
        private string _fileName;

        public void Add(ByteArray key, ByteArray value) {
            _writer.Write7BitEncodedInt(key.Length);
            _writer.Write(key.InternalBytes);
            _writer.Write7BitEncodedInt(value.Length);
            _writer.Write(value.InternalBytes);
        }

        public void Close() {
            if (_writer != null)
                _writer.Close();
            _writer = null;
        }

        public void Delete() {
            File.Delete(_fileName);
        }
    }
}
