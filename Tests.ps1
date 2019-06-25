Import-Module (Get-Module DB).Path -Force -DisableNameChecking

Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf

Remove-DBTable DBTest -Table Cluster -Confirm:$false
New-DBTable DBTest -Table Cluster -Definition {
    Define-DBColumn ClusterId int -Required -PrimaryKey
    Define-DBColumn ClusterName nvarchar -Required -Length 15
    Define-DBColumn ClusterType nvarchar
}

@"
ClusterId,ClusterName,ClusterType
1,SQL001,SQL
2,SQL002,SQL
3,CAFile,File
4,TXFile,File
6,SQL003,SQL
"@ | ConvertFrom-Csv | Add-DBRow DBTest -Table Cluster

Get-DBRow DBTest -Table Cluster -Verbose | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterEq @{ClusterType='SQL'} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterEq @{ClusterType='SQL','File'} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterNe @{ClusterType='SQL','File'} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterLike @{ClusterType='SQL','File'} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterNotLike @{ClusterType='SQL','File'} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterGt @{ClusterId=1} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterGe @{ClusterId=1} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterLt @{ClusterId=1} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterLe @{ClusterId=1} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterGe @{ClusterId=1} -FilterLe @{ClusterId=3} | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterNull ClusterType | Out-Default

Get-DBRow DBTest -Table Cluster -Verbose -FilterNotNull ClusterType | Out-Default

Remove-DBRow DBTest -Table Cluster -Verbose -FilterEq @{ClusterId=1} | Out-Default

Remove-DBRow DBTest -Table Cluster -Verbose | Out-Default # Should prompt for confirmation

Write-Host -ForegroundColor Magenta "Starting Transaction Test (Undo)"
Use-DBTransaction DBTest -Verbose
Remove-DBRow DBTest -Table Cluster -Verbose -FilterLike @{ClusterId="%"} | Out-Default
Undo-DBTransaction DBTest
Get-DBRow DBTest -Table Cluster | Out-Default


Write-Host -ForegroundColor Magenta "Starting Transaction Test (Complete)"
Use-DBTransaction DBTest -Verbose
Remove-DBRow DBTest -Table Cluster -Verbose -FilterLike @{ClusterId="%"} | Out-Default
Complete-DBTransaction DBTest
Get-DBRow DBTest -Table Cluster | Out-Default

New-DBDatabase DBTest -Database TestTemp -FileName C:\Temp\TestTemp.mdf
Get-DBDatabase DBTest | Out-Default
Remove-DBDatabase DBTest -Database TestTemp -Confirm:$false


New-DBSchema DBTest -Schema TempSchema
Get-DBSchema DBTest | Where-Object Name -eq TempSchema | Out-Default
Remove-DBSchema DBTest -Schema TempSchema -Confirm:$false


@"
ClusterId,ClusterName,ClusterType
1,SQL001,SQL
2,SQL002,SQL
3,CAFile,File
4,TXFile,File
6,SQL003,SQL
7,ORFile,File
"@ | ConvertFrom-Csv | Add-DBRow DBTest -Table Cluster -BulkCopy

Get-DBRow DBTest -Table Cluster | Out-Default

Use-DBTransaction DBTest
@"
ClusterId,ClusterName,ClusterType
8,WAFile,File
"@ | ConvertFrom-Csv | Add-DBRow DBTest -Table Cluster -BulkCopy
Complete-DBTransaction DBTest

Set-DBRow DBTest -Table Cluster -Set @{ClusterType='Unknown'} -Verbose
Get-DBRow DBTest -Table Cluster | Out-Default

Set-DBRow DBTest -Table Cluster -Set @{ClusterType='SQL'} -FilterLike @{ClusterName='SQL%'} -Verbose
Get-DBRow DBTest -Table Cluster | Out-Default

Get-DBPrimaryKey DBTest
Get-DBPrimaryKey DBTest -Table Cluster -Verbose
Get-DBPrimaryKey DBTest -Schema dbo -Verbose
Get-DBPrimaryKey DBTest -Table Cluster -AsStringArray