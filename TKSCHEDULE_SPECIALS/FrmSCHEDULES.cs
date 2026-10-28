using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using TKITDLL;


namespace TKSCHEDULE_SPECIALS
{
    public partial class FrmSCHEDULES : Form
    {
        // 來源資料庫 (來客計數器)
        private const string SOURCE_PATH = @"X:\kldatabase.db";
        // 複製到目標磁碟的資料夾與檔名
        private const string TARGET_FOLDER = "TEMP";
        private const string TARGET_FILENAME = "kldatabase_IMPORT.db";

        // 宣告一個旗標用來記錄目前是否正在執行同步（避免 Timer 與按鈕重複觸發）
        private bool isExecuting = false;
        // SQL 指令逾時秒數
        int TIMEOUT_LIMITS = 240;

        // 寫 Log 用的鎖，避免同時寫檔衝突
        private static readonly object logLock = new object();

        // TKMK 連線字串 (解密一次後快取)
        private string tkmkConnectionString = null;


        public FrmSCHEDULES()
        {
            InitializeComponent();
        }
        private void FrmSCHEDULES_Load(object sender, EventArgs e)
        {
            LoadAvailableDrives();
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
        /// 寫入 Log 檔 (取代 MessageBox，避免排程執行時跳視窗卡住)
        /// 位置：執行檔目錄\LOG\yyyyMMdd.log，一天一個檔
        /// </summary>
        private void WriteLog(string level, string message, Exception ex = null)
        {
            try
            {
                string logDir = Path.Combine(Application.StartupPath, "LOG");
                if (!Directory.Exists(logDir))
                {
                    Directory.CreateDirectory(logDir);
                }

                string logFile = Path.Combine(logDir, DateTime.Now.ToString("yyyyMMdd") + ".log");

                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("[{0}] [{1}] {2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), level, message);
                if (ex != null)
                {
                    sb.AppendLine();
                    sb.Append(ex.ToString());
                }
                sb.AppendLine();

                lock (logLock)
                {
                    File.AppendAllText(logFile, sb.ToString(), Encoding.UTF8);
                }
            }
            catch
            {
                // 寫 Log 失敗時不再往外丟，避免影響排程
            }
        }

        private void LogInfo(string message)
        {
            WriteLog("INFO", message);
        }

        private void LogError(string message, Exception ex = null)
        {
            WriteLog("ERROR", message, ex);
        }

        /// <summary>
        /// 取得 TKMK 連線字串 (帳密解密)，只解密一次
        /// </summary>
        private string GetTKMKConnectionString()
        {
            if (tkmkConnectionString == null)
            {
                //20210902密
                Class1 TKID = new Class1();//用new 建立類別實體
                SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings["dbconnTKMK"].ConnectionString);

                //資料庫使用者密碼解密
                sqlsb.Password = TKID.Decryption(sqlsb.Password);
                sqlsb.UserID = TKID.Decryption(sqlsb.UserID);

                tkmkConnectionString = sqlsb.ConnectionString;
            }

            return tkmkConnectionString;
        }

        /// <summary>
        /// 自動偵測本機可用硬碟並填入 ComboBox 下拉選單
        /// </summary>
        private void LoadAvailableDrives()
        {
            comboBoxDrives.Items.Clear();

            var drives = DriveInfo.GetDrives()
                .Where(d => d.IsReady && d.DriveType == DriveType.Fixed)
                .ToList();

            int dDriveIndex = -1; // 用來記錄 D 槽在下拉選單中的索引位置

            for (int i = 0; i < drives.Count; i++)
            {
                var drive = drives[i];
                long freeSpaceGb = drive.TotalFreeSpace / 1024 / 1024 / 1024;

                // 顯示例如 "C:\ (可用: 120 GB)"
                comboBoxDrives.Items.Add($"{drive.Name} (可用: {freeSpaceGb} GB)");

                // 檢查這個磁碟是不是 D 槽 (不分大小寫)
                if (drive.Name.StartsWith("D", StringComparison.OrdinalIgnoreCase))
                {
                    dDriveIndex = i;
                }
            }

            // 決定預設選取哪一個
            if (dDriveIndex != -1)
            {
                // 如果有找到 D 槽，就預設選 D 槽
                comboBoxDrives.SelectedIndex = dDriveIndex;
            }
            else if (comboBoxDrives.Items.Count > 0)
            {
                // 如果沒有 D 槽，才退而求其次預設選擇第一個
                comboBoxDrives.SelectedIndex = 0;
            }
        }

        /// <summary>
        /// 取得目前選擇磁碟的目標檔案路徑，例如 D:\TEMP\kldatabase_IMPORT.db
        /// 沒有選擇磁碟時回傳 null
        /// </summary>
        private string GetTargetPath()
        {
            if (comboBoxDrives.SelectedItem == null)
            {
                return null;
            }

            // 從下拉選單字串中解析出槽代號（例如從 "C:\ (可用...)" 取出 "C:\"）
            string selectedText = comboBoxDrives.SelectedItem.ToString();
            string driveLetter = selectedText.Substring(0, 3);

            return Path.Combine(driveLetter, TARGET_FOLDER, TARGET_FILENAME);
        }

        /// <summary>
        /// 排程與按鈕共用的轉入流程：複製資料庫 → 轉入來客資料
        /// </summary>
        /// <param name="trigger">觸發來源 (排程 / 手動)，寫 Log 用</param>
        private async Task RunImportAsync(string trigger)
        {
            // 如果上一趟還沒跑完，直接跳過 (排程與手動共用同一個旗標)
            if (isExecuting)
            {
                LogInfo(trigger + "：上一次轉入尚未完成，本次略過。");
                return;
            }

            isExecuting = true; // 設定為正在執行
            button1.Enabled = false;

            try
            {
                string targetPath = GetTargetPath();
                if (targetPath == null)
                {
                    lblStatus.Text = "請先選擇目標磁碟！";
                    LogError(trigger + "：未選擇目標磁碟，本次略過。");
                    return;
                }

                lblStatus.Text = "準備中...";

                bool isSuccess = await CopyDatabaseAsync(SOURCE_PATH, targetPath);

                if (!isSuccess)
                {
                    lblStatus.Text = "複製失敗！";
                    LogError(trigger + "：資料庫複製失敗，請檢查網路連線或檔案狀態。來源：" + SOURCE_PATH + "，目標：" + targetPath);
                    return;
                }

                lblStatus.Text = "複製完成，轉入資料中...";

                // 讀 SQLite、寫 SQL Server 放到背景執行，避免畫面卡住
                int importCount = await Task.Run(() => ADDTKMKt_visitors(targetPath));

                if (importCount >= 0)
                {
                    lblStatus.Text = $"完成！本次轉入 {importCount} 筆 ({DateTime.Now:yyyy-MM-dd HH:mm:ss})";
                    LogInfo($"{trigger}：完成，本次轉入 {importCount} 筆。");
                }
                else
                {
                    lblStatus.Text = "轉入失敗！請查看 LOG";
                }
            }
            catch (Exception ex)
            {
                lblStatus.Text = "執行失敗！請查看 LOG";
                LogError(trigger + " 發生錯誤", ex);
            }
            finally
            {
                // 無論成功或失敗，執行完畢後一定要把旗標改回 false，好讓下次 Timer 可以正常執行
                isExecuting = false;
                button1.Enabled = true;
            }
        }

        /// <summary>
        /// 每天 1 小時執行1次
        /// 0801
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private async void timer1_Tick(object sender, EventArgs e)
        {
            await RunImportAsync("排程執行");
        }

        /// <summary>
        /// 轉入資料來客-X:\kldatabase.db
        /// 回傳轉入筆數，失敗回傳 -1
        /// (在背景執行緒執行，不可直接存取畫面控制項)
        /// </summary>
        public int ADDTKMKt_visitors(string targetPath)
        {
            try
            {
                string MAX_Fdate = FIND_TKMKt_visitorsMAX_Fdate();

                if (string.IsNullOrEmpty(MAX_Fdate))
                {
                    LogInfo("轉入來客資料：查無 TKMK.t_visitors 最大 Fdate，本次未轉入");
                    return 0;
                }

                DataTable table = new DataTable();

                // 唯讀開啟複製出來的 SQLite 檔
                string connectionString = $"Data Source={targetPath};Version=3;Read Only=True;";

                // 用 using，用完會自動關閉連線並釋放檔案佔用
                using (SQLiteConnection sqliteConnection = new SQLiteConnection(connectionString))
                using (SQLiteCommand cmd = sqliteConnection.CreateCommand())
                {
                    // 改用參數，避免字串組 SQL
                    cmd.CommandText = @"
                                        SELECT *
                                        FROM t_visitors
                                        WHERE 1=1
                                        AND (Fin_data >= 1 OR Fout_data >= 1)
                                        AND [Fdate] >= @MAX_Fdate
                                        ";
                    cmd.Parameters.AddWithValue("@MAX_Fdate", MAX_Fdate);

                    sqliteConnection.Open();

                    using (SQLiteDataAdapter adapter = new SQLiteDataAdapter(cmd))
                    {
                        adapter.Fill(table);
                    }
                } // 離開這個範圍，SQLiteConnection 就會自動 Close 且釋放檔案

                if (table.Rows.Count == 0)
                {
                    LogInfo("轉入來客資料：沒有新資料 (Fdate >= " + MAX_Fdate + ")");
                    return 0;
                }

                // 查詢條件是 Fdate >= 最大日期，最大日期那一刻的資料上次已轉入過
                // 先排除 TKMK 已存在的 (Fdevice_sn + Fdate)，避免重複寫入
                int sourceCount = table.Rows.Count;
                REMOVE_EXISTS_ROWS(table, MAX_Fdate);

                if (table.Rows.Count == 0)
                {
                    LogInfo($"轉入來客資料：沒有新資料 (讀到 {sourceCount} 筆，皆已存在)");
                    return 0;
                }

                if (!ADDTOTKMKt_visitors(table))
                {
                    return -1;
                }

                UPDATEt_visitors();

                return table.Rows.Count;
            }
            catch (Exception EX)
            {
                LogError("轉入來客資料 (ADDTKMKt_visitors) 發生錯誤，檔案：" + targetPath, EX);
                return -1;
            }
        }

        /// <summary>
        /// 排除 TKMK.t_visitors 已存在的資料 (以 Fdevice_sn + Fdate 判斷，與刪除重複資料的規則相同)
        /// </summary>
        private void REMOVE_EXISTS_ROWS(DataTable table, string MAX_Fdate)
        {
            HashSet<string> existsKeys = new HashSet<string>();

            using (SqlConnection sqlConn = new SqlConnection(GetTKMKConnectionString()))
            using (SqlCommand cmd = new SqlCommand(@"
                                        SELECT [Fdevice_sn], [Fdate]
                                        FROM [TKMK].[dbo].[t_visitors] WITH(NOLOCK)
                                        WHERE [Fdate] >= @MAX_Fdate
                                        ", sqlConn))
            {
                cmd.CommandTimeout = TIMEOUT_LIMITS;
                cmd.Parameters.Add("@MAX_Fdate", SqlDbType.DateTime).Value = Convert.ToDateTime(MAX_Fdate);

                sqlConn.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        existsKeys.Add(MAKE_KEY(reader["Fdevice_sn"], reader["Fdate"]));
                    }
                }
            }

            if (existsKeys.Count == 0)
            {
                return;
            }

            // 由後往前刪，避免索引位移
            for (int i = table.Rows.Count - 1; i >= 0; i--)
            {
                DataRow dr = table.Rows[i];
                if (existsKeys.Contains(MAKE_KEY(dr["Fdevice_sn"], dr["Fdate"])))
                {
                    table.Rows.RemoveAt(i);
                }
            }
        }

        /// <summary>
        /// 組出比對用的 Key：Fdevice_sn|yyyy-MM-dd HH:mm:ss
        /// </summary>
        private static string MAKE_KEY(object deviceSn, object fdate)
        {
            string sn = deviceSn == null || deviceSn == DBNull.Value ? "" : deviceSn.ToString().Trim();
            string date = "";

            if (fdate != null && fdate != DBNull.Value)
            {
                DateTime dt;
                if (fdate is DateTime)
                {
                    date = ((DateTime)fdate).ToString("yyyy-MM-dd HH:mm:ss");
                }
                else if (DateTime.TryParse(fdate.ToString(), out dt))
                {
                    date = dt.ToString("yyyy-MM-dd HH:mm:ss");
                }
                else
                {
                    date = fdate.ToString().Trim();
                }
            }

            return sn + "|" + date;
        }

        // 簡化版的 UPDATEt_visitors，不需要複雜的 Transaction 也能安全執行
        public void UPDATEt_visitors()
        {
            try
            {
                using (SqlConnection sqlConn = new SqlConnection(GetTKMKConnectionString()))
                using (SqlCommand cmd = sqlConn.CreateCommand())
                {
                    cmd.CommandTimeout = TIMEOUT_LIMITS;
                    cmd.CommandText = @"
                                        UPDATE [TKMK].[dbo].[t_visitors]
                                        SET [TT002] = [t_STORESNAME].[TT002],
                                            [STORESNAME] = [t_STORESNAME].[STORESNAME]
                                        FROM [TKMK].[dbo].[t_visitors]
                                        INNER JOIN [TKMK].[dbo].[t_STORESNAME]
                                            ON [t_STORESNAME].[Fdevice_sn] = [t_visitors].[Fdevice_sn]
                                        WHERE [t_STORESNAME].[ISUSED] = 'Y'
                                          AND ISNULL([t_visitors].[TT002], '') = ''
                                    ";

                    sqlConn.Open();

                    // 單一 UPDATE 語法不需要手動宣告 Transaction，ADO.NET 會自動處理
                    int result = cmd.ExecuteNonQuery();
                    LogInfo($"更新門市名稱 (UPDATEt_visitors)：{result} 筆");
                }
            }
            catch (Exception ex)
            {
                LogError("更新門市名稱 (UPDATEt_visitors) 發生錯誤", ex);
            }
        }

        /// <summary>
        /// 查詢 TKMK.t_visitors 目前最大的 Fdate (yyyy-MM-dd HH:mm:ss)，查不到或錯誤回傳 null
        /// </summary>
        public string FIND_TKMKt_visitorsMAX_Fdate()
        {
            try
            {
                using (SqlConnection sqlConn = new SqlConnection(GetTKMKConnectionString()))
                using (SqlCommand cmd = new SqlCommand(@"
                                    SELECT
                                    CONVERT(VARCHAR(19), MAX([Fdate]), 120) AS [Fdate]
                                    FROM  [TKMK].[dbo].[t_visitors] WITH(NOLOCK)
                                    ", sqlConn))
                {
                    cmd.CommandTimeout = TIMEOUT_LIMITS;
                    sqlConn.Open();

                    object value = cmd.ExecuteScalar();
                    if (value == null || value == DBNull.Value)
                    {
                        return null;
                    }

                    return value.ToString();
                }
            }
            catch (Exception ex)
            {
                LogError("查詢 TKMK.t_visitors 最大 Fdate (FIND_TKMKt_visitorsMAX_Fdate) 發生錯誤", ex);
                return null;
            }
        }

        /// <summary>
        /// 用 SqlBulkCopy 寫入 TKMK.t_visitors，成功回傳 true
        /// </summary>
        public bool ADDTOTKMKt_visitors(DataTable dtt_visitors)
        {
            using (SqlConnection connection = new SqlConnection(GetTKMKConnectionString()))
            {
                connection.Open();
                using (SqlTransaction sqlTrans = connection.BeginTransaction())
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, sqlTrans))
                {
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
                    bulkCopy.BulkCopyTimeout = TIMEOUT_LIMITS;

                    try
                    {
                        bulkCopy.WriteToServer(dtt_visitors);
                        sqlTrans.Commit();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        sqlTrans.Rollback();
                        LogError("寫入 TKMK.t_visitors (ADDTOTKMKt_visitors) 失敗，已 Rollback，筆數：" + dtt_visitors.Rows.Count, ex);
                        return false;
                    }
                }
            }
        }

        public void DELETE_UPDATEt_visitors()
        {
            try
            {
                using (SqlConnection sqlConn = new SqlConnection(GetTKMKConnectionString()))
                using (SqlCommand cmd = sqlConn.CreateCommand())
                {
                    // ====================================================================
                    // 第一個 SQL 語句: 刪除重複的 id (保留 id 最小的那筆)
                    // 第二個 SQL 語句: 執行資料更新
                    // ====================================================================
                    cmd.CommandText = @"
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
                                            -- 改為可用索引的日期條件
                                            Fdate >= '20251001'
                                    )
                                    DELETE FROM t
                                    FROM [TKMK].[dbo].[t_visitors] t
                                    INNER JOIN CTE_DuplicateRemover cte
                                        ON t.id = cte.id
                                    WHERE cte.rn > 1;

                                    -- 步驟 2: 執行 JOIN 更新
                                    UPDATE [TKMK].[dbo].[t_visitors]
                                    SET [TT002]= [t_STORESNAME].[TT002],[STORESNAME]=[t_STORESNAME].[STORESNAME]
                                    FROM [TKMK].[dbo].[t_STORESNAME]
                                    WHERE [t_STORESNAME].[Fdevice_sn]=[t_visitors].[Fdevice_sn]
                                    AND [t_STORESNAME].[ISUSED]='Y'
                                    AND ISNULL([t_visitors].[TT002],'')='';
                                ";
                    cmd.CommandTimeout = TIMEOUT_LIMITS;

                    sqlConn.Open();
                    using (SqlTransaction tran = sqlConn.BeginTransaction())
                    {
                        cmd.Transaction = tran;
                        int result = cmd.ExecuteNonQuery();

                        if (result == 0)
                        {
                            tran.Rollback();    //交易取消
                        }
                        else
                        {
                            tran.Commit();      //執行交易
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("刪除重複/更新來客資料 (DELETE_UPDATEt_visitors) 發生錯誤", ex);
            }
        }

        /// <summary>
        /// 清除異常時間來客，回傳更新筆數，失敗回傳 -1
        /// </summary>
        public int UPDATE_visitors()
        {
            try
            {
                //觀光夜間時間段來客數據清零  (DATEPART(HOUR, [Fdate])>=20 OR DATEPART(HOUR, [Fdate])<=06)
                //門市間時間段來客數據清零 (DATEPART(HOUR, [Fdate])>=22 OR DATEPART(HOUR, [Fdate])<=07)
                using (SqlConnection sqlConn = new SqlConnection(GetTKMKConnectionString()))
                using (SqlCommand command = new SqlCommand(@"
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
                                    ", sqlConn))
                {
                    command.CommandTimeout = TIMEOUT_LIMITS;
                    sqlConn.Open();
                    int result = command.ExecuteNonQuery();
                    LogInfo($"夜間來客數清零 (UPDATE_visitors)：{result} 筆");
                    return result;
                }
            }
            catch (Exception ex)
            {
                LogError("夜間來客數清零 (UPDATE_visitors) 發生錯誤", ex);
                return -1;
            }
        }

        /// <summary>
        /// 核心複製 Function：刪除舊檔、背景複製、確認檔案大小與來源一致
        /// </summary>
        private async Task<bool> CopyDatabaseAsync(string sourcePath, string targetPath, int timeoutSeconds = 300)
        {
            try
            {
                if (!File.Exists(sourcePath))
                {
                    LogError("來源檔案不存在 (請確認 X 槽是否已連線)：" + sourcePath);
                    return false;
                }

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

                // 3. 放到背景執行複製，避免 WinForms 畫面卡死；超過 timeoutSeconds 視為逾時
                Task copyTask = Task.Run(() =>
                {
                    File.Copy(sourcePath, targetPath, overwrite: true);
                });

                Task finished = await Task.WhenAny(copyTask, Task.Delay(timeoutSeconds * 1000));
                if (finished != copyTask)
                {
                    LogError("檔案複製超時（超過 " + timeoutSeconds + " 秒），請檢查網路或檔案大小！來源：" + sourcePath + "，目標：" + targetPath);
                    return false;
                }

                // 複製過程的錯誤在這裡拋出
                await copyTask;

                // 4. File.Copy 完成即代表寫入完畢，只需確認大小與來源一致
                long sourceSize = new FileInfo(sourcePath).Length;
                long targetSize = new FileInfo(targetPath).Length;
                if (targetSize <= 0 || targetSize != sourceSize)
                {
                    LogError($"複製後檔案大小不一致，來源：{sourceSize} bytes，目標：{targetSize} bytes");
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("刪除或複製檔案時發生錯誤，來源：" + sourcePath + "，目標：" + targetPath, ex);
                return false;
            }
        }

        #endregion

        #region BUTTON
        private async void button1_Click(object sender, EventArgs e)
        {
            await RunImportAsync("手動執行");
        }

        private async void button2_Click(object sender, EventArgs e)
        {
            button2.Enabled = false;
            lblStatus.Text = "清除異常時間來客中...";

            int result = await Task.Run(() => UPDATE_visitors());

            if (result >= 0)
            {
                lblStatus.Text = $"清除異常時間來客：完成，共 {result} 筆 ({DateTime.Now:yyyy-MM-dd HH:mm:ss})";
            }
            else
            {
                lblStatus.Text = "清除異常時間來客：失敗！請查看 LOG";
            }
            button2.Enabled = true;
        }

        #endregion


    }
}
