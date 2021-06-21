using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Management;
using System.Reflection;
using System.ServiceProcess;

namespace dagit
{
    [RunInstaller(true)]
    public partial class RunDagster : ServiceBase
    {

        private Logger logger = LogManager.GetLogger("dagsterLogger");
        private List<Process> proc;
        private string machine_name;
        public RunDagster()
        {
            this.CanStop = true;
            this.CanShutdown = true;
            proc = new List<Process>();
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

            machine_name = Environment.GetEnvironmentVariable("DAGSTER_SERVICE").ToString().ToUpper().Trim();
            string path = System.IO.Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            string arg = "";

            try
            {

                if (machine_name == "DAGIT")
                {
                    arg = @"& " + path + @"\dagit.ps1";
                    StartProcess(arg);
                }
                else if (machine_name == "DAGSTER")
                {
                    arg = @"& " + path + @"\dagster.ps1";
                    StartProcess(arg);
                }
                else if (machine_name == "BOTH")
                {

                    arg = @"& " + path + @"\dagit.ps1";
                    StartProcess(arg);

                    arg = @"& " + path + @"\dagster.ps1";
                    StartProcess(arg);
                }
                else
                {
                    throw new ArgumentException("Invalid value set for DAGSTER_SERVICE environment variable");
                }
            }
            catch (Exception e)
            {
                logger.Error("Exception: " + e.Message);
                logger.Error("Exception: " + e.StackTrace);
            }

        }
        protected override void OnStop()
        {
            foreach (Process p in proc)
            {
                EndProcessTree(p.Id);
            }
        }

        private void StartProcess(string args)
        {
            Process start_proc = new Process();
            start_proc.StartInfo.FileName = @"powershell.exe";
            start_proc.StartInfo.Arguments = args;
            start_proc.StartInfo.RedirectStandardOutput = true;
            start_proc.StartInfo.RedirectStandardError = true;
            start_proc.StartInfo.UseShellExecute = false;

            start_proc.OutputDataReceived += ConsoleOutputHandler;
            start_proc.StartInfo.RedirectStandardInput = true;
            start_proc.Start();
            proc.Add(start_proc);
           
            start_proc.BeginOutputReadLine();
        }
        private void ConsoleOutputHandler(object sendingProcess,
            DataReceivedEventArgs e)
        {
            logger.Debug(e.Data);
        }

        private void EndProcessTree(int pid)
        {
            ManagementObjectSearcher searcher = new ManagementObjectSearcher("Select * From Win32_Process Where ParentProcessID=" + pid);
            ManagementObjectCollection moc = searcher.Get();
            foreach (ManagementObject mo in moc)
            {
                EndProcessTree(Convert.ToInt32(mo["ProcessID"]));
            }
            try
            {
                Process proc = Process.GetProcessById(pid);
                proc.Kill();
            }
            catch (ArgumentException)
            { /* process already exited */ }
        }
    }
}
