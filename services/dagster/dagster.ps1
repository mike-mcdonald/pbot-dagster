$pythonEnvPath = 'D:\dagster\dagster-venv\Scripts\Activate.ps1'
Invoke-Expression $pythonEnvPath
$path = 'D:\dagster\pbot-dagster'
Set-Location $path
$runDagster = 'dagster-daemon run'
Invoke-Expression $runDagster