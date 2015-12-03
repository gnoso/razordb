using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Text.RegularExpressions;

namespace RazorView {
    /// <summary>
    /// Interaction logic for DataViewControl.xaml
    /// </summary>
    public partial class DataViewControl : UserControl, IDisposable {
        public DataViewControl() {
            InitializeComponent();
            //dataGrid.SetValue(ScrollViewer.CanContentScrollProperty, false);
            dataGrid.EnableRowVirtualization = true;
        }

        public void Dispose() {
            Close();
        }

        private DBController _db;
        public DBController DBController {
            get { return _db; }
            set { _db = value; if (_db != null) RefreshData(); }
        }

        public void Close() {
            if (DBController != null) {
                DBController.Close();
                DBController = null;
            }
        }

        private IEnumerable<Record> _originalSource;
        public void RefreshData() {
            if (_originalSource == null) {
                _originalSource = DBController.GetRecords(null, null);
            }

            var rOpts = RegexOptions.IgnorePatternWhitespace| RegexOptions.CultureInvariant | RegexOptions.Compiled | RegexOptions.Multiline;
            var keyRegex = string.IsNullOrEmpty(KeyFilterTextBox.Text) ? null : new Regex(KeyFilterTextBox.Text.Trim(), rOpts);
            var valRegex = string.IsNullOrEmpty(ValueFilterTextBox.Text) ? null : new Regex(ValueFilterTextBox.Text.Trim(), rOpts);
            var matches = new Func<string, string, bool>((key, val) => {
                return (keyRegex == null || keyRegex.IsMatch(Regex.Escape(key))) && (valRegex == null || valRegex.IsMatch(Regex.Escape(val)));
            });
            dataGrid.ItemsSource = _originalSource.Where(r => matches(r.Key, r.Value));
        }

        private void RefreshButton_Click(object sender, RoutedEventArgs e) {
            RefreshData();
        }

    }
}
