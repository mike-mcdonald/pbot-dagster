# Dagster Windows Service Application

A small windows service application that runs powershell scripts to run dagit (dagster UI) and/or dagster daemon as a service.

## Building the service

Set the build option to Release in visual studio and Build the service application, it should create service executable files under `pbot-dagster\services\dagster\bin\Release`

## Installing the service

Open a Command line window and navigate to `C:\Windows\Microsoft.NET\Framework\v4.0.30319`
Run the following command to install:
`installutil.exe -i path\to\project\services\dagster\bin\Release\dagster.exe`

## Set DAGSTER_SERVICE System Environment Variable

Options:
1. DAGIT: To run dagit (Dagster UI) as service.
2. DAGSTER: To run dagster as service.
3. BOTH: To run both dagit and dagster as service.

## PowerShell Scripts

Verify the project path and python environment path in dagit.ps1 and dagster.ps1 in `path\to\project\services\dagster\bin\Release`

You can also modify the host and port that dagit will run on.

Set the Dagster service startup to Automatic and start the service.

browse dagit at http://127.0.0.1:3000