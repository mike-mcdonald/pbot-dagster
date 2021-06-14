$pythonEnvPath = 'D:\Apps\dagster\dagster-venv\Scripts\Activate.ps1'
Invoke-Expression $pythonEnvPath
$path = 'D:\Apps\dagster\dagster'
Set-Location $path
$runDagit = 'dagit -h 0.0.0.0 -p 3000'
Invoke-Expression $runDagit