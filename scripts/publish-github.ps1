param(
  [Parameter(Mandatory = $true)]
  [string]$RemoteUrl
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path '.git')) { throw 'Run this script from the repository root.' }
if ((git status --porcelain) -ne '') { throw 'Working tree must be clean before publishing.' }

$existing = git remote get-url origin 2>$null
if ($LASTEXITCODE -eq 0 -and $existing -ne $RemoteUrl) {
  throw "origin already points to $existing; refusing to overwrite it."
}
if ($LASTEXITCODE -ne 0) { git remote add origin $RemoteUrl }

git push -u origin main
git push origin v0.1.0
Write-Output "Published main and v0.1.0 to $RemoteUrl"
