/* 
Copyright 2012 Gnoso Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

namespace RazorDB {
    public class TableManager {

        public TableManager(RazorCache cache, Manifest mf) {
            _manifest = mf;
            _cache = cache;
            _closing = false;
            _managerThread = new Thread(ThreadMain);
            _managerThread.Start();
        }

        private Thread _managerThread;
        private Manifest _manifest;
        private RazorCache _cache;
        private bool _closing;

        // The locking protocol assumes that only one TableManager thread is running per database. If there is more than one thread
        // running, then they will step all over each other.
        private void ThreadMain() {

            while (true) {
                bool mergedDuringLastPass = false;
                try {
                    using (var manifest = _manifest.GetLatestManifest()) {
                        // Handle level 0 (merge all pages)
                        if (manifest.GetNumPagesAtLevel(0) >= Config.MaxPagesOnLevel(0)) {
                            mergedDuringLastPass = true;
                            var inputPageRecords = manifest.GetPagesAtLevel(0).Take(Config.MaxPagesOnLevel(0)).ToList();
                            var startKey = inputPageRecords.Min(p => p.FirstKey);
                            var endKey = inputPageRecords.Max(p => p.LastKey);
                            var mergePages = manifest.FindPagesForKeyRange(1, startKey, endKey).AsPageRefs().ToList();
                            var allInputPages = inputPageRecords.AsPageRefs().Concat(mergePages).ToList();

                            var outputPages = SortedBlockTable.MergeTables(_cache, _manifest, 1, allInputPages).ToList();
                            _manifest.ModifyPages(outputPages, allInputPages);

                            _manifest.LogMessage("Merge Level 0 => InputPages: {0} OutputPages:{1}",
                                string.Join(",", allInputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray()),
                                string.Join(",", outputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray())
                            );
                        }
                        // handle the rest of the levels (merge only one page upwards)
                        for (int level = 1; level < manifest.NumLevels - 1; level++) {
                            if (manifest.GetNumPagesAtLevel(level) >= Config.MaxPagesOnLevel(level)) {
                                mergedDuringLastPass = true;
                                var inputPage = _manifest.NextMergePage(level);
                                var mergePages = manifest.FindPagesForKeyRange(level + 1, inputPage.FirstKey, inputPage.LastKey).ToList();
                                var allInputPages = mergePages.Concat(new PageRecord[] { inputPage }).AsPageRefs().ToList();
                                var outputPages = SortedBlockTable.MergeTables(_cache, _manifest, level + 1, allInputPages);
                                _manifest.ModifyPages(outputPages, allInputPages);

                                _manifest.LogMessage("Merge Level >0 => InputPages: {0} OutputPages:{1}",
                                    string.Join(",", allInputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray()),
                                    string.Join(",", outputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray())
                                );
                            }
                        }
                    }
                } catch (Exception e) {
                    Console.WriteLine("Error in TableManager: {0}", e);
                }

                if (!mergedDuringLastPass && _closing)
                    break;

                // Sleep for a bit, this needs to be upgraded to something a bit smarter....
                if (!mergedDuringLastPass)
                    Thread.Sleep(200);
            }
        }

        public void Close() {
            _closing = true;
            _managerThread.Join();
        }
    }
}
