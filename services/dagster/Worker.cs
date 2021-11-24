using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace dagster;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly WorkerOptions _options;
    private Process _process;

    public Worker(ILogger<Worker> logger, IOptions<WorkerOptions> options)
    {
        _logger = logger;
        _options = options.Value;
        _process = new Process();
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(_options.Command))
        {
            _logger.LogCritical("Dagster service failed to start. No command argument passed in.");
            throw new ArgumentNullException("You must pass an argument for the command to run! Either 'dagit' or 'daemon'");
        }

        List<String> arguments = new List<string>();

        var currentDirectory = System.IO.Path.GetDirectoryName(System.AppDomain.CurrentDomain.BaseDirectory);

        if (String.IsNullOrEmpty(currentDirectory))
        {
            throw new ArgumentNullException("Could not determine current directory!");
        }

        arguments.Add(Path.Combine(currentDirectory, "entrypoint.ps1"));
        if (!String.IsNullOrEmpty(_options.VirtualEnv)) arguments.Add($"-VirtualEnv {_options.VirtualEnv}");
        if (!String.IsNullOrEmpty(_options.Directory)) arguments.Add($"-Directory {_options.Directory}");
        arguments.Add($"-Command {_options.Command}");

        _process.StartInfo.FileName = @"powershell.exe";
        _process.StartInfo.Arguments = String.Join(" ", arguments);
        _process.StartInfo.RedirectStandardOutput = true;
        _process.StartInfo.RedirectStandardError = true;
        _process.StartInfo.UseShellExecute = false;
        _process.StartInfo.RedirectStandardInput = true;

        DataReceivedEventHandler HandleOutput = (object sendingProcess, DataReceivedEventArgs e) => _logger.LogInformation(e.Data);

        _process.OutputDataReceived += HandleOutput;
        _process.ErrorDataReceived += HandleOutput;

        _process.Start();
        _process.BeginErrorReadLine();
        _process.BeginOutputReadLine();

        _logger.LogInformation("Worker started at: {time}", DateTimeOffset.Now);

        await base.StartAsync(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker stopping at: {time}", DateTimeOffset.Now);
        _process.Kill(true);

        await base.StopAsync(cancellationToken);
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogTrace("Worker running at: {time}", DateTimeOffset.Now);
            await _process.WaitForExitAsync(stoppingToken);
        }
    }
}
