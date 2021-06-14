using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace dagit
{
    [RunInstaller(true)]
    public partial class RunDagster : ServiceBase
    {


        private List<Process> proc;
        private string machine_name;
        public RunDagster()
        {
            this.CanStop = true;
            this.CanShutdown = true;
            machine_name = Environment.MachineName.ToString().ToUpper().Trim();
            proc = new List<Process>();
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

            string path = System.IO.Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            string arg = "";

            try
            {

                if (machine_name == "PBOTWEB4")
                {
                    arg = @"& " + path + @"\dagit.ps1";
                    StartProcess(arg);
                }
                else if (machine_name == "PBOTDM1")
                {
                    arg = @"& " + path + @"\dagster.ps1";
                    StartProcess(arg);
                }
                else
                {

                    arg = @"& " + path + @"\dagit.ps1";
                    StartProcess(arg);

                    arg = @"& " + path + @"\dagster.ps1";
                    StartProcess(arg);
                }
            }
            catch (Exception e)
            {
                string log_file_path = @"C:\dagster\log.txt";
                if (File.Exists(log_file_path)){
                    File.Delete(log_file_path);
                }
                using (StreamWriter writer = File.CreateText(log_file_path))
                {
                    writer.WriteLine(e.Message);
                    writer.WriteLine(e.StackTrace);
                    writer.WriteLine(e.InnerException);
                }
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

            start_proc.Start();
            proc.Add(start_proc);
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
