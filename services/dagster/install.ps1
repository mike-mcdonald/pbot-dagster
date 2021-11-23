$binaryPath = Join-Path $PWD "dagster.exe" | Resolve-Path
if (-not $(Test-Path $binaryPath)) {
    throw "dagster.exe not in this directory. Aborting installation!"
}

$daemon = New-Object System.Management.Automation.Host.ChoiceDescription 'Dagi&t'
$dagit = New-Object System.Management.Automation.Host.ChoiceDescription 'Dae&mon'
$both = New-Object System.Management.Automation.Host.ChoiceDescription '&Both'
$options = [System.Management.Automation.Host.ChoiceDescription[]]($dagit, $daemon, $both)

$title = 'Service(s)'
$message = 'Which service would you like to install?'

$services = switch ($host.ui.PromptForChoice($title, $message, $options, 0)) {
    0 { @("daemon") }
    1 { @("dagit") }
    2 { @("daemon", "dagit") }
    Default { throw "Invalid service option!" }
}

$requiredVariables = @(
    "DAGSTER_POSTGRES_HOST",
    "DAGSTER_POSTGRES_PORT",
    "DAGSTER_POSTGRES_DB",
    "DAGSTER_POSTGRES_USERNAME",
    "DAGSTER_POSTGRES_PASSWORD",
    "DAGSTER_AES_KEY"
);

foreach ($variable in $requiredVariables) {
    if (-not [Environment]::GetEnvironmentVariable($variable)) {
        Write-Host "Environment variable '$variable' not found!"
        $value = Read-Host -Prompt "Enter value for '$variable'" -AsSecureString
        $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($value)
        $value = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($BSTR)
        [Environment]::SetEnvironmentVariable($variable, $value, 'Machine')
        Set-Item -Path "Env:$variable" -Value $value
    }
}

if (-not [Environment]::GetEnvironmentVariable('DAGSTER_HOME')) {
    Write-Host "Environment variable 'DAGSTER_HOME' not found!"
    $value = Read-Host -Prompt "Enter value for 'DAGSTER_HOME'"
    [Environment]::SetEnvironmentVariable('DAGSTER_HOME', $value, 'Machine')
    Set-Item -Path "Env:DAGSTER_HOME" -Value $value
}

$virtualEnv = Read-Host -Prompt "Enter the path where python executables are installed, usually a virtual environment"
$virtualEnv = Resolve-Path $virtualEnv

$directory = Read-Host -Prompt "Enter the directory where you want the $service service to start from"
$directory = Resolve-Path $directory

$credential = Get-Credential -Message "Enter the credentials to run the service"

$TextInfo = (Get-Culture).TextInfo

foreach ($service in $services) {
    $binaryPath = $binaryPath.ToString() + " --VirtualEnv $virtualEnv --Directory $directory --Command $service"

    Write-Host "Creating '$service' service as '$binaryPath'..."

    $name = $TextInfo.ToTitleCase($service)

    New-Service -Name "Dagster$name" `
        -DisplayName "Dagster $name" `
        -Description "Service managing dagster's $name process" `
        -BinaryPathName $binaryPath `
        -Credential $credential `
        -StartupType "AutomaticDelayedStart"
}

