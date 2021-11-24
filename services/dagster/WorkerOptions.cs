namespace dagster;

public class WorkerOptions
{
    public String Command { get; set; } = String.Empty;
    public String VirtualEnv { get; set; } = String.Empty;
    public String Directory { get; set; } = String.Empty;
}