param (
    [string]$VirtualEnv = $(throw "-VirtualEnv is required."),
    [string]$Directory = $pwd,
    [string]$Command = $(throw "-Command is required.")
)
Write-Host "Switching to '$Directory'..."
Set-Location $Directory
Switch ($Command) {
    "dagit" { $Command = "dagster-webserver"; $Arguments = "-l /apps/dagster -p 3030" }
    "daemon" { $Command = "dagster-daemon"; $Arguments = "run" }
    default { $(throw "You must specify either 'dagit' or 'dagster.'") }
}
$Command = Join-Path $VirtualEnv "Scripts\$Command.exe"
$Env:PYTHONDONTWRITEBYTECODE = 1
$Env:CRYPTOGRAPHY_OPENSSL_NO_LEGACY = 1
Write-Host "Running '$Command'..."
Invoke-Expression "$Command $Arguments"