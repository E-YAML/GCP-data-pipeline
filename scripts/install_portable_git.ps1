[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$zipPath = Join-Path $env:TEMP "mingit.zip"
$destPath = Join-Path $env:LOCALAPPDATA "PortableGit"

Write-Host "Downloading MinGit Portable..."
Invoke-WebRequest -Uri "https://github.com/git-for-windows/git/releases/download/v2.44.0.windows.1/MinGit-2.44.0-64-bit.zip" -OutFile $zipPath

Write-Host "Extracting MinGit to $destPath..."
if (Test-Path $destPath) { Remove-Item $destPath -Recurse -Force }
Expand-Archive -Path $zipPath -DestinationPath $destPath -Force
Remove-Item $zipPath

Write-Host "PortableGit successfully installed!"
