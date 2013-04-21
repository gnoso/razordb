using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace RazorDB {
    /// <summary>
    /// Provides Reader/Writer locks around files and maintains opened reader FileStreams to speed file read operations.
    /// Allows multiple threads to safely work with the same files.
    /// Allows one or more asynchronous readers to be opened on a file but only a single writer. Readers are blocked while a 
    /// writer lock is in progress.
    /// Recursive lock acquisition is allowed, but readers cannot escalate into writers as this pattern is deadlock prone.
    /// The ReaderWriterLockSlim class is utilized to manage inter-thread file locking.
    /// For more information see: http://msdn.microsoft.com/en-us/library/system.threading.readerwriterlockslim(v=vs.90).aspx
    /// </summary>
    /// <example>
    /// string filePath = "...some file path...";
    /// FileStream fileStream = null;
    /// using(var fileManager = new SortedBlockTableFileManager()) {
    ///     try {
    ///         fileStream = fileManager.BeginRead(filePath);
    ///         
    ///         // Reader/Writer synchronized code here
    ///         
    ///     } finally {
    ///         // releases the Reader lock
    ///         fileManager.EndRead(filePath, fs);
    ///     }
    /// } // all opened Readers are closed - IMPORTANT: All Locks must be released (using EndRead/EndWrite) before disposing.
    /// 
    /// </example>
    public class SortedBlockTableFileManager : IDisposable {

        /// <summary>
        /// How long to block the current Thread and wait for a Read lock before timing out (in milliseconds).
        /// </summary>
        public const int DefaultReaderLockTimeout = 5000;

        /// <summary>
        /// How long to block the current Thread and wait for a Write lock before timing out (in milliseconds).
        /// </summary>
        public const int DefaultWriterLockTimeout = 8000;

        class OpenFile {
            public string Path;
            public FileStream Stream;
        }
        
        class SBTFile {
            public ReaderWriterLockSlim Lock;
            public Stack<FileStream> Readers;

            public SBTFile(string path) {
                if (string.IsNullOrEmpty(path)) {
                    throw new ArgumentException("A file path must be specified.", "path");
                }
                Lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
                Readers = new Stack<FileStream>();
            }
        }

        IList<OpenFile> _openFiles;
        IDictionary<string, SBTFile> _files;

        public SortedBlockTableFileManager() {
            _openFiles = new List<OpenFile>();
            _files = new Dictionary<string, SBTFile>();
        }

        SBTFile _EnsureSBTFile(string path) {
            SBTFile returnValue = null;
            lock(_files) {
                if (!_files.ContainsKey(path)) {
                    returnValue = new SBTFile(path);
                    _files.Add(path, returnValue);
                } else {
                    returnValue = _files[path];
                }
            }
            return returnValue;
        }

        /// <summary>
        /// Acquire a read lock on a file and open a new read-only FileStream.
        /// The current Thread is blocked until the read lock timeout expires after which a TimeoutException is thrown.
        /// IMPORTANT: the returned FileStream should not be closed but should be passed back to the EndRead method.
        /// The FileStream is disposed when the file manager is disposed (or when CloseFile is called directly).
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public FileStream BeginRead(string path) {
            FileStream returnValue = null;
            SBTFile file = _EnsureSBTFile(path);
            if(file.Lock.TryEnterReadLock(DefaultReaderLockTimeout)) {
                lock(file.Readers) {
                    if(0 == file.Readers.Count) {
                        // create a new reader
                        returnValue = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, Config.SortedBlockSize, FileOptions.Asynchronous);
                        // keep track of all opened file streams so they can be closed on Disposal
                        _openFiles.Add(new OpenFile() {
                                Path = path,
                                Stream = returnValue
                            });
                    } else {
                        // return an existing reader
                        returnValue = file.Readers.Pop();
                        returnValue.Seek(0, SeekOrigin.Begin);
                    }
                }
                return returnValue;
            } else {
                throw new TimeoutException("Failed to acquire Read lock on file: " + path);
            }
        }

        /// <summary>
        /// Release a read lock and return the FileStream to the open files pool.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="fs"></param>
        public void EndRead(string path, FileStream fs) {
            SBTFile file = _EnsureSBTFile(path);
            file.Lock.ExitReadLock();
            if (null != fs) {
                lock(file.Readers) {
                    file.Readers.Push(fs);
                }
            }
        }
        
        /// <summary>
        /// Acquire a write lock on a file and open a new writable FileStream.
        /// The current Thread is blocked until the write lock timeout expires after which a TimeoutException is thrown.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public FileStream BeginWrite(string path) {
            SBTFile file = _EnsureSBTFile(path);
            if(file.Lock.TryEnterWriteLock(DefaultWriterLockTimeout)) {
                try {
                    return new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
                } catch {
                    // Immediately release the write lock and re-throw. This prevents write locks from leaking.
                    file.Lock.ExitWriteLock();
                    throw;
                }
            } else {
                throw new TimeoutException("Failed to acquire Write lock on file: " + path);
            }
        }

        /// <summary>
        /// Closes the file stream and releases the writer lock.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="fs"></param>
        public void EndWrite(string path, FileStream fs) {
            SBTFile file = _EnsureSBTFile(path);
            try {
                // SafeHandle flushes stream and releases locks
                fs.SafeFileHandle.Dispose();
                fs.Dispose();
            } finally {
                file.Lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Closes all open file streams for a given file path and then disposes the read/write locks.
        /// Also acquires a write lock and allows an action to be performed on the file before the lock is exposed.
        /// This prevents any other threads from grabbing a read/write lock on the file before the action is completed.
        /// </summary>
        /// <param name="path"></param>
        public void SafeCloseFileAction(string path, Action safeAction) {
            lock (_files) {
                if (_files.ContainsKey(path)) {
                    var sbtFile = _files[path];
                    // cloase each reader
                    var readers = _files[path].Readers;
                    lock (readers) {
                        for (int i = 0; i < _openFiles.Count; i++) {
                            if (_openFiles[i].Path.Equals(path, StringComparison.OrdinalIgnoreCase)) {
                                try {
                                    _openFiles[i].Stream.Dispose();
                                } catch { ; }
                            }
                        }
                        _openFiles = _openFiles.Where(f => !f.Path.Equals(path, StringComparison.OrdinalIgnoreCase)).ToList();
                    }

                    try {
                        if (null != safeAction) {
                            // acquire write lock and perform action
                            if (sbtFile.Lock.TryEnterWriteLock(DefaultWriterLockTimeout)) {
                                try {
                                    safeAction();
                                } finally {
                                    // cleanup lock
                                    sbtFile.Lock.ExitWriteLock();
                                }
                            }
                        }
                    } finally {
                        sbtFile.Lock.Dispose();
                        _files.Remove(path);
                    }
                }
            }
        }

        bool _disposed;

        public void Dispose() {
            _Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Closes all opened reader streams and disposes read/write locks.
        /// </summary>
        /// <param name="disposing"></param>
        void _Dispose(bool disposing) {
            if (!_disposed) {
                lock (_openFiles) {
                    try {
                        foreach (OpenFile fs in _openFiles) {
                            try {
                                fs.Stream.Dispose();
                            } catch { ; }
                        }
                        _openFiles.Clear();
                    } finally {
                        foreach (SBTFile sbtFile in _files.Values) {
                            sbtFile.Lock.Dispose();
                        }
                        _files.Clear();
                    }
                }

                _disposed = true;
            }
        }

        ~SortedBlockTableFileManager() {
            _Dispose(false);
        }
    }
}
