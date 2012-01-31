using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    public static class Config {

        public static int MaxMemTableSize = 1 * 1024 * 1024; // Maximum size we should let the memtable grow to in memory before compacting.
    }
}
