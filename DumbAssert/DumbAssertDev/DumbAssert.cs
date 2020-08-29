using System;
using System.Collections.Generic;
using Microsoft.VisualBasic.FileIO;
using System.Text;
using System.IO;
using System.Linq;
using System.Data;

namespace DumbAssertNS
{
    public class DumbAssert
    {
        public IDbConnection Connection { get; set; }

        public IDbTransaction Transaction { get; set; }

        public Dictionary<string, TestData> Repository { get; set; }

        public DumbAssert(IDbConnection connection = null, IDbTransaction transaction = null)
        {
            this.Connection = connection;
            this.Transaction = transaction;
        }

        public void Prepare(string testId)
        {
            ExecuteSQL(LoadOrGetTestData(testId).GeneratePrepareSQL());
        }

        public void Assert(string testId)
        {
            TestData testdata = LoadOrGetTestData(testId);
            string templateSelect = "SELECT {1} FROM {0} ORDER BY {1}";
            foreach(var exp in testdata.Expected)
            {
                bool ownedTransaction = (null == this.Transaction);
                using(IDbTransaction tx = (ownedTransaction)? this.Connection.BeginTransaction() : this.Transaction)
                using(IDbCommand cmd = this.Connection.CreateCommand())
                {
                    cmd.CommandText = string.Format(templateSelect, exp.TableName, string.Join(",", exp.Columns));
                    cmd.Transaction = tx;
                    using(IDataReader reader = cmd.ExecuteReader())
                    {
                        DataTable tbl = new DataTable();
                        tbl.Load(reader);
                        AssertTable(testdata, exp, tbl);
                    }
                    if (ownedTransaction)
                    {
                        tx.Rollback();
                    }
                }
            }
        }

        private static void AssertTable(TestData testdata, TableData exp, DataTable act)
        {
            for (int r = 0; r < exp.Rows.Count; r++)
            {
                for (int c = 0; c < exp.Columns.Count(); c++)
                {
                    string valExp = exp.Rows[r][c];
                    string valAct = DumbAssertConfig.Serializer.Serialize(
                        act.Rows[r].Field<object>(exp.Columns[c]));
                    if (!(valExp == valAct))
                    {
                        throw new Exception(
                            string.Format("AssertTable failed Table:{5} Row:{2} Column:{3} Expected:[{0}] Actual:[{1}] ExpectedSource:{4}",
                                valExp, valAct, r, exp.Columns[c], testdata.DirPath, exp.TableName));
                    }
                }
            }
        }

        private string FindTestDataDirById(string testId)
        {
            string[] subDirs = Directory.GetDirectories(
                DumbAssertConfig.TestDataBaseDir, 
                DumbAssertConfig.PrefixTestData 
                    + DumbAssertConfig.PathDelimiter 
                    + testId 
                    + DumbAssertConfig.PathDelimiter
                    + "*", 
                System.IO.SearchOption.AllDirectories);
            return subDirs[0];
        }

        private TestData LoadOrGetTestData(string testId)
        {
            if(null == this.Repository)
            {
                this.Repository = new Dictionary<string, TestData>();
            }

            TestData testdata = null;
            this.Repository.TryGetValue(testId, out testdata);

            if(null == testdata)
            {
                testdata = new TestData(FindTestDataDirById(testId));
                this.Repository.Add(testId, testdata);
                return testdata;
            }

            return testdata;
        }

        private void ExecuteSQL(string sql) 
        {
            bool ownedTransaction = (null == this.Transaction);
            using(var tx = (ownedTransaction)? this.Connection.BeginTransaction() : this.Transaction)
            using(var cmd = this.Connection.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
                if(ownedTransaction)
                {
                    tx.Commit();
                }
            }
        }
    }

    public class DumbAssertSerializer
    {
        public string Serialize(object value)
        {
            switch(value)
            {
                case Boolean val: return Serialize(val);
                case Byte val: return Serialize(val);
                case Char val: return Serialize(val);
                case DateTime val: return Serialize(val);
                case Decimal val: return Serialize(val);
                case Double val: return Serialize(val);
                case Guid val: return Serialize(val);
                case Int16 val: return Serialize(val);
                case Int32 val: return Serialize(val);
                case Int64 val: return Serialize(val);
                case SByte val: return Serialize(val);
                case Single val: return Serialize(val);
                case String val: return Serialize(val);
                case TimeSpan val: return Serialize(val);
                case UInt16 val: return Serialize(val);
                case UInt32 val: return Serialize(val);
                case UInt64 val: return Serialize(val);
                case Byte[] val: return Serialize(val);
                default: return null;
            }
        }
        public string Serialize(Boolean value) { return value.ToString(); }
        public string Serialize(Byte value) { return value.ToString(); }
        public string Serialize(Char value) { return value.ToString(); }
        public string Serialize(DateTime value) { return value.ToString("yyyy-MM-dd HH:mm:ss.fff"); }
        public string Serialize(Decimal value) { return value.ToString(); }
        public string Serialize(Double value) { return value.ToString(); }
        public string Serialize(Guid value) { return value.ToString(); }
        public string Serialize(Int16 value) { return value.ToString(); }
        public string Serialize(Int32 value) { return value.ToString(); }
        public string Serialize(Int64 value) { return value.ToString(); }
        public string Serialize(SByte value) { return value.ToString(); }
        public string Serialize(Single value) { return value.ToString(); }
        public string Serialize(String value) { return value.ToString(); }
        public string Serialize(TimeSpan value) { return value.ToString(); }
        public string Serialize(UInt16 value) { return value.ToString(); }
        public string Serialize(UInt32 value) { return value.ToString(); }
        public string Serialize(UInt64 value) { return value.ToString(); }
        public string Serialize(Byte[] value) { return value.ToString(); }
    }

    public class DumbAssertConfig
    {
        public static string PathDelimiter = "__";

        public static string PrefixTestData = "T";

        public static string PrefixPrerequisite = "R";

        public static string PrefixExpected = "E";

        public static string TestDataExtension = "csv";

        public static bool DeleteBeforeInsert = true;

        public static string NewLine = Environment.NewLine;

        public static string TestDataBaseDir = null;

        public static DumbAssertSerializer Serializer = new DumbAssertSerializer();
    }

    public class TestData
    {
        public string TestId { get; set; }

        public string Desc { get; set; }

        public string DirPath { get; set; }

        public List<TableData> Prerequisite;

        public List<TableData> Expected;

        public TestData(string dirPath)
        {
            // Set test info
            string[] testInfo = Path.GetFileName(dirPath).Split(DumbAssertConfig.PathDelimiter);
            this.TestId = testInfo[1];
            this.Desc = testInfo[2];
            this.DirPath = Path.GetFullPath(dirPath);

            // Load prerequisite
            this.Prerequisite = ReadTableDataAll(dirPath, DumbAssertConfig.PrefixPrerequisite);

            // Load expected
            this.Expected = ReadTableDataAll(dirPath, DumbAssertConfig.PrefixExpected);
        }

        public string GeneratePrepareSQL()
        {
            return string.Join(DumbAssertConfig.NewLine, 
                this.Prerequisite.OrderBy(s => s.TableName)
                    .Select(p => p.GenerateInsertSQL()).ToList());
        }

        private string[] GetFileList(string dirPath, string prefix)
        {
            return System.IO.Directory.GetFiles(dirPath, 
                prefix
                + DumbAssertConfig.PathDelimiter 
                + "*." + DumbAssertConfig.TestDataExtension, 
                System.IO.SearchOption.AllDirectories);
        }

        private List<TableData> ReadTableDataAll(string dirPath, string prefix)
        {
            string[] files = GetFileList(dirPath, prefix);
            List<TableData> data = new List<TableData>();
            foreach(string file in files)
            {
                data.Add(new TableData(file));
            }
            return data;
        }
    }

    public class TableData 
    {
        public string TableName { get; set; }

        public List<string[]> Data { get; set; }

        public string[] Columns { get; set; }

        public List<string[]> Rows { get; set; }

        public TableData(string filePath) 
        {
            // Read CSV
            TextFieldParser parser = new TextFieldParser(filePath, Encoding.GetEncoding("UTF-8"));
            parser.SetDelimiters(",");
            this.Data = new List<string[]>();
            while(!parser.EndOfData)
            {
                this.Data.Add(parser.ReadFields());
            }
            this.Columns = this.Data[0];
            this.Rows = this.Data.Skip(1).ToList();

            // Set table name
            this.TableName = Path.GetFileNameWithoutExtension(filePath).Split(DumbAssertConfig.PathDelimiter)[1];
        }

        public string GenerateInsertSQL()
        {
            List<string> sql = new List<string>();

            // Add delete statement
            if(DumbAssertConfig.DeleteBeforeInsert)
            {
                string templateDelete = "DELETE FROM {0};";
                sql.Add(string.Format(templateDelete, this.TableName));
            }

            // Add insert statement
            string templateInsert = "INSERT INTO {0} ({1}) VALUES ({2});";
            string columns = string.Join(",", this.Columns);
            foreach(var row in this.Rows)
            {
                string values = string.Join(",", row.ToList().Select(s => "'" + s + "'"));
                sql.Add(string.Format(templateInsert, this.TableName, columns, values));
            }

            // Join and return sql
            return string.Join(DumbAssertConfig.NewLine, sql);
        }
    }
}