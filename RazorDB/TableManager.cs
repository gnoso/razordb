using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

namespace RazorDB {
    public class TableManager {

        public TableManager(Manifest mf) {
            _manifest = mf;
            _running = true;
            _managerThread = new Thread(ThreadMain);
            _managerThread.Start();
        }

        private Thread _managerThread;
        private Manifest _manifest;
        private bool _running;

        // The locking protocol assumes that only one TableManager thread is running per database. If there is more than one thread
        // running, then they will step all over each other.
        private void ThreadMain() {

            while (_running) {
                try {
                    if (_manifest.GetNumPagesAtLevel(0) >= Config.MaxPagesOnLevel(0)) {
                        var inputPages = _manifest.GetPagesAtLevel(0).Select(pr => new PageRef { Level = 0, Version = pr.Version }).ToList();
                        var outputPages = SortedBlockTable.MergeTables(_manifest, 1, inputPages);
                        _manifest.ModifyPages(outputPages, inputPages);

                        foreach (var pageFile in inputPages) {
                            File.Delete(Config.SortedBlockTableFile(_manifest.BaseFileName, pageFile.Level, pageFile.Version));
                        }
                    }
                } catch (Exception e) {
                    Console.WriteLine("Error in TableManager: {0}", e);
                }

                // Sleep for a bit, this needs to be upgraded to something a bit smarter....
                Thread.Sleep(0);
            }
        }

        public void Close() {
            _running = false;
        }
    }
}
