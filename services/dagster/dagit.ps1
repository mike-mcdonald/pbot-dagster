$pythonEnvPath = 'D:\Apps\dagster\dagster-venv\Scripts\Activate.ps1'
Invoke-Expression $pythonEnvPath
$path = 'D:\Apps\dagster\pbot-dagster'
Set-Location $path
$runDagit = 'dagit -l /dagster -h 0.0.0.0 -p 3000'
Invoke-Expression $runDagit