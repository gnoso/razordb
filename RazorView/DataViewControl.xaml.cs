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

namespace RazorView {
    /// <summary>
    /// Interaction logic for DataViewControl.xaml
    /// </summary>
    public partial class DataViewControl : UserControl, IDisposable {
        public DataViewControl() {
            InitializeComponent();
        }

        public void Dispose() {
            Close();
        }

        DBController _db;
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

        public void RefreshData() {
            dataGrid.ItemsSource = DBController.GetRecords(KeyFilterTextBox.Text, ValueFilterTextBox.Text);
        }

        void RefreshButton_Click(object sender, RoutedEventArgs e) {
            RefreshData();
        }

    }
}
