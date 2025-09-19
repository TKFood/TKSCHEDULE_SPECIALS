using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;
using System.Configuration;
using FastReport;
using FastReport.Data;
using System.Threading;
using System.Xml;
using System.Xml.Linq;
using System.Xml;
using System.Xml.Linq;
using TKITDLL;
using System.Text.RegularExpressions;

using System.Data.SQLite;


namespace TKSCHEDULE_SPECIALS
{
    public partial class FrmSCHEDULES : Form
    {

        int TIMEOUT_LIMITS = 240;

               
        public FrmSCHEDULES()
        {
            InitializeComponent();
        }

        #region FUNCTION
        private void timer1_Tick(object sender, EventArgs e)
        {

        }
        /// <summary>
        /// 轉入資料來客-X:\kldatabase.db
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>        
        public void ADDTKMKt_visitors()
        {
            SqlConnection sqlConn = new SqlConnection();
            SqlCommand sqlComm = new SqlCommand();
            string connectionString;
            StringBuilder sbSql = new StringBuilder();
            SqlTransaction tran;

            int result;

            SQLiteConnection SQLiteConnection = new SQLiteConnection();

            //string MAXID = null;
            string MAX_Fdate = null;

            try
            {
                //MAXID = FINDTKMKt_visitorsMAXID();
                MAX_Fdate = FIND_TKMKt_visitorsMAX_Fdate();

                if (!string.IsNullOrEmpty(MAX_Fdate))
                {
                    //SQLite的檔案要先copy到 F:\kldatabase.db
                    // string path = @"data source=E:\kldatabase.db";
                    string path = @"data source=X:\kldatabase.db";
                    //string path = @"data source=\\192.168.1.101\Users\Administrator\AppData\Roaming\CounterServerData\kldatabase.db";

                    //string filePath = @"\\192.168.1.101\Users\Administrator\AppData\Roaming\CounterServerData\kldatabase.db";
                    //if (File.Exists(filePath))
                    //{
                    //    MessageBox.Show("存在！");
                    //}
                    //else
                    //{
                    //    MessageBox.Show("檔案不存在！");
                    //}

                    SQLiteConnection = new SQLiteConnection(path);
                    SQLiteConnection.Open();

                    SQLiteCommand cmd = SQLiteConnection.CreateCommand();

                    sbSql.Clear();
                    sbSql.AppendFormat(@"  
                                        SELECT *
                                        FROM t_visitors
                                        WHERE Fdate>='{0}'
                                     ", MAX_Fdate);

                    cmd.CommandText = sbSql.ToString();

                    // 用DataAdapter和DataTable類，記得要 using System.Data
                    SQLiteDataAdapter adapter = new SQLiteDataAdapter(cmd);
                    DataTable table = new DataTable();
                    adapter.Fill(table);

                    if (table.Rows.Count > 0)
                    {
                        ADDTOTKMKt_visitors(table);
                        UPDATEt_visitors();
                    }

                    else
                    {
                        //MessageBox.Show("沒有新資料，請更新kldatabasepri 到E:");
                    }

                    SQLiteConnection.Close();


                }
                else
                {
                    //MessageBox.Show("沒有新資料，請更新kldatabasepri 到E:");
                }

            }
            catch
            {
                //MessageBox.Show("有錯誤");
            }
            finally
            {

            }
        }
        public string FIND_TKMKt_visitorsMAX_Fdate()
        {
            SqlConnection sqlConn = new SqlConnection();
            SqlCommand sqlComm = new SqlCommand();
            string connectionString;
            StringBuilder sbSql = new StringBuilder();
            StringBuilder sbSqlQuery = new StringBuilder();
            SqlTransaction tran;
            SqlCommand cmd = new SqlCommand();
            int result;

            string Fdate = null;

            try
            {
                //20210902密
                Class1 TKID = new Class1();//用new 建立類別實體
                SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString);

                //資料庫使用者密碼解密
                sqlsb.Password = TKID.Decryption(sqlsb.Password);
                sqlsb.UserID = TKID.Decryption(sqlsb.UserID);

                sqlConn = new SqlConnection(sqlsb.ConnectionString);


                SqlDataAdapter adapter1 = new SqlDataAdapter();
                SqlCommandBuilder sqlCmdBuilder1 = new SqlCommandBuilder();
                DataSet ds1 = new DataSet();

                sbSql.Clear();
                sbSqlQuery.Clear();

                sbSql.AppendFormat(@"  
                                    SELECT 
                                    CONVERT(VARCHAR, MAX([Fdate]), 120) AS [Fdate]
                                    FROM  [TKMK].[dbo].[t_visitors]
                                    ");

                adapter1 = new SqlDataAdapter(@"" + sbSql, sqlConn);

                sqlCmdBuilder1 = new SqlCommandBuilder(adapter1);
                sqlConn.Open();
                ds1.Clear();
                adapter1.Fill(ds1, "TEMPds1");
                sqlConn.Close();


                if (ds1.Tables["TEMPds1"].Rows.Count >= 1)
                {
                    Fdate = ds1.Tables["TEMPds1"].Rows[0]["Fdate"].ToString();
                }
                else
                {

                }

            }
            catch
            {

            }
            finally
            {
                sqlConn.Close();
            }

            return Fdate;
        }

        public void ADDTOTKMKt_visitors(DataTable dtt_visitors)
        {
            SqlConnection sqlConn = new SqlConnection();
            SqlCommand sqlComm = new SqlCommand();
            string connectionString;
            StringBuilder sbSql = new StringBuilder();
            SqlTransaction tran;
            SqlCommand cmd = new SqlCommand();
            int result;

            //20210902密
            Class1 TKID = new Class1();//用new 建立類別實體
            SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["dbconnTKMK"].ConnectionString);

            //資料庫使用者密碼解密
            sqlsb.Password = TKID.Decryption(sqlsb.Password);
            sqlsb.UserID = TKID.Decryption(sqlsb.UserID);

            sqlConn = new SqlConnection(sqlsb.ConnectionString);

            using (SqlConnection connection = sqlConn)
            {
                connection.Open();
                SqlTransaction sqlTrans = connection.BeginTransaction();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, sqlTrans))
                {
                    DataTable dt = dtt_visitors;
                    bulkCopy.DestinationTableName = "t_visitors";

                    //對應資料行
                    //bulkCopy.ColumnMappings.Add("DataTable的欄位A", "資料庫裡的資料表的的欄位A");
                    bulkCopy.ColumnMappings.Add("Fuid", "Fuid");
                    bulkCopy.ColumnMappings.Add("Fvisit_md5", "Fvisit_md5");
                    bulkCopy.ColumnMappings.Add("Fdevice_sn", "Fdevice_sn");
                    bulkCopy.ColumnMappings.Add("Fdate", "Fdate");
                    bulkCopy.ColumnMappings.Add("Fin_data", "Fin_data");
                    bulkCopy.ColumnMappings.Add("Fout_data", "Fout_data");
                    bulkCopy.ColumnMappings.Add("Fcreate_time", "Fcreate_time");
                    bulkCopy.ColumnMappings.Add("Fdata_version", "Fdata_version");
                    bulkCopy.ColumnMappings.Add("Fbatvoltage", "Fbatvoltage");
                    bulkCopy.ColumnMappings.Add("Fbatpercent", "Fbatpercent");
                    bulkCopy.ColumnMappings.Add("Flosefocus", "Flosefocus");
                    bulkCopy.ColumnMappings.Add("Fcharge", "Fcharge");
                    bulkCopy.ColumnMappings.Add("Ftemperature", "Ftemperature");
                    bulkCopy.ColumnMappings.Add("id", "id");

                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 60;

                    try
                    {
                        bulkCopy.WriteToServer(dt);
                        sqlTrans.Commit();

                        //MessageBox.Show("完成");
                    }

                    catch (Exception)
                    {
                        sqlTrans.Rollback();
                    }

                }

            }
        }

        public void UPDATEt_visitors()
        {
            SqlConnection sqlConn = new SqlConnection();
            SqlCommand sqlComm = new SqlCommand();
            string connectionString;
            StringBuilder sbSql = new StringBuilder();
            SqlTransaction tran;
            SqlCommand cmd = new SqlCommand();
            int result;

            try
            {
                //20210902密
                Class1 TKID = new Class1();//用new 建立類別實體
                SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["dbconnTKMK"].ConnectionString);

                //資料庫使用者密碼解密
                sqlsb.Password = TKID.Decryption(sqlsb.Password);
                sqlsb.UserID = TKID.Decryption(sqlsb.UserID);

                sqlConn = new SqlConnection(sqlsb.ConnectionString);


                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();

                sbSql.AppendFormat(@"
                                     UPDATE [TKMK].[dbo].[t_visitors]
                                    SET [TT002]= [t_STORESNAME].[TT002],[STORESNAME]=[t_STORESNAME].[STORESNAME]
                                    FROM [TKMK].[dbo].[t_STORESNAME]
                                    WHERE [t_STORESNAME].[Fdevice_sn]=[t_visitors].[Fdevice_sn]
                                    AND [t_STORESNAME].[ISUSED]='Y'
                                    AND ISNULL([t_visitors].[TT002],'')=''
                                    ");



                cmd.Connection = sqlConn;
                cmd.CommandTimeout = 60;
                cmd.CommandText = sbSql.ToString();
                cmd.Transaction = tran;
                result = cmd.ExecuteNonQuery();

                if (result == 0)
                {
                    tran.Rollback();    //交易取消


                }
                else
                {
                    tran.Commit();      //執行交易                    

                }

            }
            catch
            {

            }

            finally
            {
                sqlConn.Close();
            }


        }
        #endregion

        #region BUTTON
        private void button1_Click(object sender, EventArgs e)
        {
            //轉入資料來客-X:\kldatabase.db
            //要指定來客記錄的db的磁碟-X:\kldatabase.db
            //X=\\192.168.1.101\Users\Administrator\AppData\Roaming\CounterServerData

            ADDTKMKt_visitors();
            MessageBox.Show("OK");
        }

        #endregion

       
    }
}
