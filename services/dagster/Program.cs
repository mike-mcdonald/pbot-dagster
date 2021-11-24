using NLog.Extensions.Logging;

using dagster;

IHost host = Host
    .CreateDefaultBuilder(args)
    .UseWindowsService()
    .ConfigureLogging((context, logging) =>
    {
        logging.ClearProviders();
        logging.AddEventLog();
        logging.AddNLog();
        logging.AddConfiguration(context.Configuration);
    })
    .ConfigureServices((context, services) =>
    {
        services.Configure<WorkerOptions>(options => context.Configuration.Bind(options));
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
