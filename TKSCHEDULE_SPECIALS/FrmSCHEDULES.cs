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
        private void FrmSCHEDULES_Load(object sender, EventArgs e)
        {
            // 啟用計時器
            timer1.Enabled = true;

            // *** 關鍵修改：將間隔設為 60 分鐘 ***
            // 1000 毫秒/秒 * 60 秒/分鐘 * 60 分鐘/小時 = 3,600,000 毫秒
            timer1.Interval = 1000 * 60 * 60; // 1 小時

            timer1.Start();

            // 可以在這裡手動執行一次，確保程式啟動時立刻執行
            timer1_Tick(null, null);
        }
        #region FUNCTION

        /// <summary>
        /// 每天 1 小時執行1次
        /// 0801
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void timer1_Tick(object sender, EventArgs e)
        {            
            try
            {
                // 從下拉選單字串中解析出槽代號（例如從 "C:\ (可用...)" 取出 "C:\"）
                string selectedText = comboBoxDrives.SelectedItem.ToString();
                string driveLetter = selectedText.Substring(0, 3); // 取得 "C:\" 或 "D:\"

                // 決定目標路徑：存放在該槽底下的 temp 資料夾
                string targetDirectory = Path.Combine(driveLetter, "TEMP");
                string targetPath = Path.Combine(targetDirectory, "kldatabase_IMPORT.db");

                // 執行您的主要任務
                ADDTKMKt_visitors(targetPath);
                UPDATE_visitors();
            }
            catch (Exception ex)
            {
            }          
        }
        /// <summary>
        /// 轉入資料來客-X:\kldatabase.db
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>        
        public void ADDTKMKt_visitors(string targetPath)
        {
            StringBuilder sbSql = new StringBuilder();
            string MAX_Fdate = null;

            try
            {
                MAX_Fdate = FIND_TKMKt_visitorsMAX_Fdate();

                if (!string.IsNullOrEmpty(MAX_Fdate))
                {
                    // 💡 修正 1：必須組成正確的連線字串格式 (Data Source=...)
                    string connectionString = $"Data Source={targetPath};Version=3;";

                    // 💡 建議使用 using，用完會自動關閉連線並釋放檔案佔用
                    using (SQLiteConnection sqliteConnection = new SQLiteConnection(connectionString))
                    {
                        sqliteConnection.Open();

                        using (SQLiteCommand cmd = sqliteConnection.CreateCommand())
                        {
                            sbSql.Clear();
                            sbSql.AppendFormat(@"
                                            SELECT * 
                                            FROM t_visitors 
                                            WHERE 1=1 
                                            AND (Fin_data >= 1 OR Fout_data >= 1)                                            
                                            AND [Fdate] >= '{0}'
                                        ", MAX_Fdate);

                            cmd.CommandText = sbSql.ToString();

                            // 用 DataAdapter 和 DataTable 填入資料
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
                                MessageBox.Show("沒有新資料！");
                            }
                        }
                    } // 離開這個範圍，SQLiteConnection 就會自動 Close 且釋放檔案
                }
                else
                {
                    MessageBox.Show("沒有新資料！");
                }
            }
            catch (Exception EX)
            {
                MessageBox.Show("有錯誤:" + EX.Message);
            }
        }
        public void UPDATEt_visitors()
        {
            SqlConnection sqlConn = new SqlConnection();
            SqlCommand sqlComm = new SqlCommand();
            SqlTransaction tran;
            SqlCommand cmd = new SqlCommand();
            int result;
            StringBuilder sbSql = new StringBuilder();

            try
            {
                //20210902密
                Class1 TKID = new Class1();//用new 建立類別實體
                SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString);

                //資料庫使用者密碼解密
                sqlsb.Password = TKID.Decryption(sqlsb.Password);
                sqlsb.UserID = TKID.Decryption(sqlsb.UserID);

                String connectionString;
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
                SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["dbconnTKMK"].ConnectionString);

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

        public void DELETE_UPDATEt_visitors()
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
                // ====================================================================
                // 第一個 SQL 語句: 刪除重複的 id (保留 id 最小的那筆)
                // ====================================================================
                sbSql.AppendFormat(@"                                  
                                    WITH CTE_DuplicateRemover AS (
                                        SELECT 
                                            id,
                                            -- PARTITION BY Fdevice_sn 和 Fdate 的組合，確保只對「真正重複的資料」編號
                                            -- ORDER BY id ASC 確保保留 id 最小（通常是插入最早）的那筆
                                            ROW_NUMBER() OVER (
                                                PARTITION BY Fdevice_sn, Fdate 
                                                ORDER BY id ASC
                                            ) AS rn 
                                        FROM 
                                            [TKMK].[dbo].[t_visitors]
                                        WHERE  
                                            CONVERT(NVARCHAR, Fdate, 112) >= '20251001'
                                    )
                                    DELETE FROM t
                                    FROM [TKMK].[dbo].[t_visitors] t
                                    INNER JOIN CTE_DuplicateRemover cte 
                                        ON t.id = cte.id
                                    WHERE cte.rn > 1;
                                ");

                // ====================================================================
                // 第二個 SQL 語句: 執行資料更新
                // ====================================================================
                sbSql.AppendFormat(@"
                                    -- 步驟 2: 執行 JOIN 更新
                                    UPDATE [TKMK].[dbo].[t_visitors]
                                    SET [TT002]= [t_STORESNAME].[TT002],[STORESNAME]=[t_STORESNAME].[STORESNAME]
                                    FROM [TKMK].[dbo].[t_STORESNAME]
                                    WHERE [t_STORESNAME].[Fdevice_sn]=[t_visitors].[Fdevice_sn]
                                    AND [t_STORESNAME].[ISUSED]='Y'
                                    AND ISNULL([t_visitors].[TT002],'')='';
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

        public void UPDATE_visitors()
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

                StringBuilder SQL_EXE = new StringBuilder();

                //觀光夜間時間段來客數據清零  (DATEPART(HOUR, [Fdate])>=20 OR DATEPART(HOUR, [Fdate])<=06)
                //門市間時間段來客數據清零 (DATEPART(HOUR, [Fdate])>=22 OR DATEPART(HOUR, [Fdate])<=07)
                SQL_EXE.AppendFormat(@"
                                        UPDATE [TKMK].[dbo].[t_visitors]
                                        SET [Fin_data]=0,[Fout_data]=0
                                        WHERE [TT002] LIKE '%1067%'
                                        AND ([Fin_data]>=1 OR [Fout_data]>=1)
                                        AND (DATEPART(HOUR, [Fdate])>=20 OR DATEPART(HOUR, [Fdate])<=06)

                                        UPDATE [TKMK].[dbo].[t_visitors]
                                        SET [Fin_data]=0,[Fout_data]=0
                                        WHERE  [TT002] LIKE '1065%'
                                        AND ([Fin_data]>=1 OR [Fout_data]>=1)
                                        AND (DATEPART(HOUR, [Fdate])>=22 OR DATEPART(HOUR, [Fdate])<=07)

                                    ");

                cmd.Connection = sqlConn;
                using (SqlCommand command = new SqlCommand(SQL_EXE.ToString(), sqlConn))
                {
                    command.CommandTimeout = 60;
                    sqlConn.Open();
                    result = command.ExecuteNonQuery();
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
        /// <summary>
        /// 核心複製 Function：刪除舊檔、背景複製、監控檔案大小穩定
        /// </summary>
        private async Task<bool> CopyDatabaseAsync(string sourcePath, string targetPath, int timeoutSeconds = 300)
        {
            try
            {
                // 1. 確保目標資料夾（temp）存在，如果不存在就自動建立
                string targetDir = Path.GetDirectoryName(targetPath);
                if (!string.IsNullOrEmpty(targetDir) && !Directory.Exists(targetDir))
                {
                    Directory.CreateDirectory(targetDir);
                }

                // 2. 每次複製前，先檢查並刪除 temp 裡的舊 db 檔案
                if (File.Exists(targetPath))
                {
                    // 如果檔案剛好被佔用或屬性唯讀，先強制解除唯讀屬性再刪除
                    File.SetAttributes(targetPath, FileAttributes.Normal);
                    File.Delete(targetPath);
                }

                lblStatus.Text = "正在從 X 槽複製檔案到 temp，請稍候（大檔案可能需要幾分鐘）...";

                // 3. 放到背景執行複製，避免 WinForms 畫面卡死
                await Task.Run(() =>
                {
                    File.Copy(sourcePath, targetPath, overwrite: true);
                });

                // 4. 大檔案防呆：確認檔案寫入完成且大小穩定（最多等待 5 分鐘）
                long lastSize = -1;
                int currentSeconds = 0;

                while (currentSeconds < timeoutSeconds)
                {
                    if (File.Exists(targetPath))
                    {
                        FileInfo fi = new FileInfo(targetPath);
                        long currentSize = fi.Length;

                        // 如果檔案大小連續兩次檢查都一樣且大於 0，代表複製完成且硬體寫入完畢
                        if (currentSize == lastSize && currentSize > 0)
                        {
                            return true;
                        }
                        lastSize = currentSize;
                    }

                    await Task.Delay(1000);
                    currentSeconds++;
                }

                // 若超過 5 分鐘還沒穩定，則視為逾時
                MessageBox.Show("檔案複製超時（超過 5 分鐘），請檢查網路或檔案大小！", "警告", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return false;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"刪除或複製檔案時發生錯誤: {ex.Message}", "錯誤", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return false;
            }
        }

        #endregion

        #region BUTTON
        private async void button1_Click(object sender, EventArgs e)
        {
            string sourcePath = @"X:\kldatabase.db";

            if (comboBoxDrives.SelectedItem == null)
            {
                MessageBox.Show("請先選擇目標磁碟！", "提示", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            // 從下拉選單字串中解析出槽代號（例如從 "C:\ (可用...)" 取出 "C:\"）
            string selectedText = comboBoxDrives.SelectedItem.ToString();
            string driveLetter = selectedText.Substring(0, 3); // 取得 "C:\" 或 "D:\"

            // 決定目標路徑：存放在該槽底下的 temp 資料夾
            string targetDirectory = Path.Combine(driveLetter, "TEMP");
            string targetPath = Path.Combine(targetDirectory, "kldatabase_IMPORT.db");

            // 💡 修正：鎖定按鈕避免重複點擊
            button1.Enabled = false;
            lblStatus.Text = "準備中...";

            // 1. 呼叫複製 Function（現在加上 async 之後，await 就可以正常運作了）
            bool isSuccess = await CopyDatabaseAsync(sourcePath, targetPath);

            if (isSuccess)
            {
                lblStatus.Text = "複製完成";

                //轉入資料   
                //string DB = "kldatabase_IMPORT";
                ADDTKMKt_visitors(targetPath);

            }
            else
            {
                lblStatus.Text = "複製失敗！";
                MessageBox.Show("資料庫複製失敗，請檢查網路連線或檔案狀態。", "錯誤", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }

            // 💡 修正：解除按鈕鎖定
            button1.Enabled = true;
        }

        private void button2_Click(object sender, EventArgs e)
        {
            UPDATE_visitors();
            MessageBox.Show("OK");

        }

        #endregion


    }
}
