param (
    [string]$VirtualEnv = $(throw "-VirtualEnv is required."),
    [string]$Directory = $pwd,
    [string]$Command = $(throw "-Command is required.")
)
Write-Host "Switching to '$Directory'..."
Set-Location $Directory
Switch ($Command) {
    "dagit" { $Command = "dagit"; $Arguments = "-l /apps/dagster -p 3030" }
    "daemon" { $Command = "dagster-daemon"; $Arguments = "run" }
    default { $(throw "You must specify either 'dagit' or 'dagster.'") }
}
$Command = Join-Path $VirtualEnv "Scripts\$Command.exe"
Write-Host "Running '$Command'..."
Invoke-Expression "$Command $Arguments"