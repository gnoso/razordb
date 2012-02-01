using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    public class Manifest {

        public Manifest(string baseFileName) {
            _baseFileName = baseFileName;
        }
        private string _baseFileName;
        public string BaseFileName {
            get { return _baseFileName; }
        }

        private int _level_0_version;
        public int Level0Version {
            get { return _level_0_version; }
        }


    }
}
