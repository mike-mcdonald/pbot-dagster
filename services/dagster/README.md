# Dagster services
This folder contains everything to run Dagster's daemon and dagit processes as a Windows service.

## Overview
This is a .NET 6 project that creates an executable wrapper around a Powershell script that starts Dagster's python executables. It contains an install and uninstall script to help set up the services and environment variables required for Dagster to run.

## Building
In order to compile this service you'll need to have the [.NET 6.0 SDK](https://dotnet.microsoft.com/download/dotnet/6.0). Once you have downloaded the SDK you can build this service byt running `dotnet restore` then `dotnet publish --configuration Release`. Once those commands have completed, you will have an output folder in `.\bin\Release\net6.0\win-x64\publish` which you can copy to your desired destination. Alternatively you can specify a directory to place the output in with the `--output <DIR>` option for `dotnet publish`.

## Deployment
You can deploy the service by copying the publish folder after completing the [Building](./#building) section above to your desired machine. Your destination does not need to have the .NET runtime installed, as the project is set up to bundle the runtime with the published output.

Once you've copied the published output to your destination, you can run the `install.ps1` script to set up the service you want by following the prompts. This script will configure the machine's environment variables, so it must be run from an elevated prompt. You can install either the dagit or daemon service or both. The services will be installed as "Dagster <Daemon/Dagit>" in the services listing and will be set up to start automatically, but must be manually started the first time.

If you wish to uninstall the services, you can run the `uninstall.ps1` script and optionally pass the `-Environment` switch to clean up all the environment variables.