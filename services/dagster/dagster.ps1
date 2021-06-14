$pythonEnvPath = 'D:\dagster\dagster-venv\Scripts\Activate.ps1'
Invoke-Expression $pythonEnvPath
$path = 'D:\dagster\dagster'
Set-Location $path
$runDagster = 'dagster-daemon run'
Invoke-Expression $runDagster