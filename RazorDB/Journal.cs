﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RazorDB {
    public class JournalWriter {
        public JournalWriter(string baseFileName, int version, bool append) {
            _fileName = Config.JournalFile(baseFileName, version);
            FileMode fileMode = append ? FileMode.Append : FileMode.Create;
            _writer = new BinaryWriter(new FileStream(_fileName, fileMode, FileAccess.Write, FileShare.None, 1024, false));
        }

        BinaryWriter _writer;
        string _fileName;
        object _writeLock = new object();

        // Add an item to the journal. It's possible that a thread is still Adding while another thread is Closing the journal.
        // in that case, we return false and expect the caller to do the operation over again on another journal instance.
        public bool Add(Key key, Value value) {
            lock (_writeLock) {
                if (_writer == null) return false;
                else {
                    _writer.Write7BitEncodedInt(key.Length);
                    _writer.Write(key.InternalBytes);
                    _writer.Write7BitEncodedInt(value.Length);
                    _writer.Write(value.InternalBytes);
                    _writer.Flush();
                    return true;
                }
            }
        }

        public void Close() {
            lock (_writeLock) {
                if (_writer != null)
                    _writer.Close();
                _writer = null;
            }
        }

        public void Delete() {
            if (File.Exists(_fileName))
                File.Delete(_fileName);
        }
    }

    public class JournalReader {
        public JournalReader(string baseFileName, int version) {
            _fileName = Config.JournalFile(baseFileName, version);
            _reader = new BinaryReader(new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.None, 1024, false));
        }

        BinaryReader _reader;
        string _fileName;

        public IEnumerable<KeyValuePair<Key, Value>> Enumerate() {
            byte[] key = null;
            byte[] value = null;
            bool data = true;
            while (data) {
                try {
                    int keyLen = _reader.Read7BitEncodedInt();
                    key = _reader.ReadBytes(keyLen);
                    if (key.Length != keyLen)
                        throw new InvalidOperationException();
                    int valueLen = _reader.Read7BitEncodedInt();
                    value = _reader.ReadBytes(valueLen);
                    if (valueLen <= 0 || valueLen != value.Length)
                        throw new InvalidOperationException();
                } catch (EndOfStreamException) {
                    data = false;
                } catch (InvalidOperationException) {
                    data = false;
                }
                if (data) yield return new KeyValuePair<Key, Value>(Key.FromBytes(key), Value.FromBytes(value));
            }
        }

        public void Close() {
            if (_reader != null)
                _reader.Close();
            _reader = null;
        }
    }
}