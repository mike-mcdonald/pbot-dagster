param (
    [switch]$Environment
)

$binaryPath = Join-Path $PWD "dagster.exe"
if (-not $(Test-Path $binaryPath)) {
    throw "dagster.exe not in this directory. Aborting installation!"
}

if ($Environment) {
    $requiredVariables = @(
        "DAGSTER_POSTGRES_HOST",
        "DAGSTER_POSTGRES_PORT",
        "DAGSTER_POSTGRES_DB",
        "DAGSTER_POSTGRES_USERNAME",
        "DAGSTER_POSTGRES_PASSWORD",
        "DAGSTER_HOME",
        "DAGSTER_AES_KEY"
    );

    foreach ($variable in $requiredVariables) {
        if ([Environment]::GetEnvironmentVariable($variable)) {
            Write-Host "Removing environment variable '$variable'..."
            [Environment]::SetEnvironmentVariable($variable, $null, 'Machine')
            Set-Item -Path "Env:$variable" -Value $null
        }
    }
}

$TextInfo = (Get-Culture).TextInfo

foreach ($service in @("dagit", "daemon")) {
    Write-Host "Removing '$service' service..."
    $name = $TextInfo.ToTitleCase($service)
    Get-Service -Name "Dagster$name" | Remove-Service
}