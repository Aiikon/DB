Import-Module (Get-Module DB).Path -Force -DisableNameChecking

Describe 'DB Module' {
    Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
    Get-DBTable DBTest | ForEach-Object { Remove-DBTable DBTest -Schema $_.Schema -Table $_.Table -Confirm:$false }
    try { Remove-DBSchema DBTest -Schema Tests -Confirm:$false -ErrorAction Stop } catch { }

    Context 'Schema Creation' {
        
        It 'New-DBSchema' {
            New-DBSchema DBTest -Schema Tests
        }

        It 'Get-DBSchema' {
            Get-DBSchema DBTest | Where-Object Schema -eq Tests | Should Not BeNullOrEmpty
        }

        It 'Remove-DBSchema' {
            Remove-DBSchema DBTest -Schema Tests -Confirm:$false
            Get-DBSchema DBTest | Where-Object Schema -eq Tests | Should BeNullOrEmpty
            New-DBSchema DBTest -Schema Tests
        }
    }

    Context 'Table Creation' {
        
        It 'New-DBTable' {

            New-DBTable DBTest -Table Cluster -Definition {
                Define-DBColumn ClusterId int -Required -PrimaryKey
                Define-DBColumn ClusterName nvarchar -Required -Length 15
                Define-DBColumn ClusterType nvarchar
            }

            Get-DBTable DBTest | Where-Object Table -eq Cluster | Should Not BeNullOrEmpty 
        }

        It 'Remove-DBTable' {

            Remove-DBTable DBTest -Table Cluster -Confirm:$false

            Get-DBTable DBTest | Where-Object Table -eq Cluster | Should BeNullOrEmpty 
        }
    }

    Context 'Table Rows' {

        New-DBTable DBTest -Table Cluster -Definition {
            Define-DBColumn ClusterId int -Required -PrimaryKey
            Define-DBColumn ClusterName nvarchar -Required -Length 15
            Define-DBColumn ClusterType nvarchar
        }

        It 'Add-DBRow' {

            "ClusterId,ClusterName,ClusterType
            1,SQL001,SQL
            2,SQL002,SQL
            3,CAFile,File
            4,TXFile,File
            6,SQL003,SQL" -replace ' ' | ConvertFrom-Csv | Add-DBRow DBTest -Table Cluster

            $rowList = Invoke-DBQuery DBTest "SELECT * FROM Tests.Cluster" | Sort-Object ClusterId
            $rowList.Count | Should Be 5
            $rowList[0].ClusterId | Should Be 1
            $rowList[0].ClusterName | Should Be SQL001
            $rowList[0].ClusterType | Should Be SQL
        }

        It 'Get-DBRow' {
            (Get-DBRow DBTest -Table Cluster).Count | Should Be 5
        }

        It 'Get-DBRow -Column' {
            $properties = Get-DBRow DBTest -Table Cluster -Column ClusterName, ClusterType |
                Select-Object -First 1 |
                ForEach-Object PSObject |
                ForEach-Object Properties

            $properties[0].Name | Should Be ClusterName
            $properties[1].Name | Should Be ClusterType
            $properties[2].Name | Should BeNullOrEmpty
        }

        It 'Get-DBRow -Column -Unique' {
            $results = Get-DBRow DBTest -Table Cluster -Column ClusterType -Unique

            @($results).Count | Should Be 2
            $results[0].PSObject.Properties.Name | Should Be ClusterType
        }

        It 'Get-DBRow -Column -Unique -Count' {
            $results = Get-DBRow DBTest -Table Cluster -Column ClusterType -Unique -Count

            @($results).Count | Should Be 2
            @($results[0].PSObject.Properties)[0].Name | Should Be ClusterType
            @($results[0].PSObject.Properties)[1].Name | Should Be Count
        }

        It 'Get-DBRow -FilterEq' {
            @(Get-DBRow DBTest -Table Cluster -FilterEq @{ClusterType='SQL'}).Count | Should Be 3
            @(Get-DBRow DBTest -Table Cluster -FilterEq @{ClusterType='SQL','File'}).Count | Should Be 5
        }

        It 'Get-DBRow -FilterNe' {
            @(Get-DBRow DBTest -Table Cluster -FilterNe @{ClusterType='SQL'}).Count | Should Be 2
            @(Get-DBRow DBTest -Table Cluster -FilterNe @{ClusterType='SQL','File'}).Count | Should Be 0
        }

        It 'Get-DBRow -FilterLike' {
            @(Get-DBRow DBTest -Table Cluster -FilterLike @{ClusterType='S%'}).Count | Should Be 3
            @(Get-DBRow DBTest -Table Cluster -FilterLike @{ClusterType='S%','F%'}).Count | Should Be 5
        }

        It 'Get-DBRow -FilterNotLike' {
            @(Get-DBRow DBTest -Table Cluster -FilterNotLike @{ClusterType='S%','F%'}).Count | Should Be 0
            @(Get-DBRow DBTest -Table Cluster -FilterNotLike @{ClusterType='S%'}).Count | Should Be 2
        }

        It 'Get-DBRow -FilterGt' {
            @(Get-DBRow DBTest -Table Cluster -FilterGt @{ClusterId=2}).Count | Should Be 3
        }

        It 'Get-DBRow -FilterGe' {
            @(Get-DBRow DBTest -Table Cluster -FilterGe @{ClusterId=2}).Count | Should Be 4
        }

        It 'Get-DBRow -FilterLt' {
            @(Get-DBRow DBTest -Table Cluster -FilterLt @{ClusterId=2}).Count | Should Be 1
        }

        It 'Get-DBRow -FilterLe' {
            @(Get-DBRow DBTest -Table Cluster -FilterLe @{ClusterId=2}).Count | Should Be 2
        }

        It 'Get-DBRow -FilterGe -FilterLe' {
            @(Get-DBRow DBTest -Table Cluster -FilterGe @{ClusterId=1} -FilterLe @{ClusterId=3}).Count | Should Be 3
        }

        It 'Get-DBRow -FilterNull' {
            @(Get-DBRow DBTest -Table Cluster -FilterNull ClusterType).Count | Should Be 0
        }

        It 'Get-DBRow -FilterNotNull' {
            @(Get-DBRow DBTest -Table Cluster -FilterNotNull ClusterType).Count | Should Be 5
        }

        It 'Get-DBRow -FilterExists' {
            $exists = "
                ClusterId,ClusterName
                1,SQL001
                2,SQLBADNAME
                3,CAFile
            ".Trim() -replace ' ' | ConvertFrom-Csv

            $results = Get-DBRow DBTest -Table Cluster -FilterExists $exists |
                Sort-Object ClusterId

            @($results).Count | Should Be 2
            $results[0].ClusterId | Should Be 1
            $results[1].ClusterId | Should Be 3
        }

        It 'Remove-DBRow -FilterEq' {
            Remove-DBRow DBTest -Table Cluster -FilterEq @{ClusterId=1}
            @(Get-DBRow DBTest -Table Cluster).Count | Should Be 4
        }

        It 'Remove-DBRow' {
            Remove-DBRow DBTest -Table Cluster -Confirm:$false
            @(Get-DBRow DBTest -Table Cluster).Count | Should Be 0
        }

        It 'Add-DBRow -BulkCopy' {

            "ClusterId,ClusterName,ClusterType
            1,SQL001,SQL
            2,SQL002,SQL
            3,CAFile,File
            4,TXFile,File
            6,SQL003,SQL" -replace ' ' | ConvertFrom-Csv | Add-DBRow DBTest -Table Cluster -BulkCopy

            $rowList = Invoke-DBQuery DBTest "SELECT * FROM Tests.Cluster" | Sort-Object ClusterId
            $rowList.Count | Should Be 5
            $rowList[0].ClusterId | Should Be 1
            $rowList[0].ClusterName | Should Be SQL001
            $rowList[0].ClusterType | Should Be SQL
        }

        It 'Get-DBRow -Sum' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterType -Unique -Sum ClusterId | Sort-Object ClusterType
            $data | ? ClusterType -eq File | ForEach-Object ClusterId | Should Be 7
            $data | ? ClusterType -eq SQL | ForEach-Object ClusterId | Should Be 9
        }

        It 'Get-DBRow -Min -Max' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterType -Unique -Min ClusterId -Max ClusterId | Sort-Object ClusterType
            $data | ? ClusterType -eq File | ForEach-Object ClusterIdMin | Should Be 3
            $data | ? ClusterType -eq File | ForEach-Object ClusterIdMax | Should Be 4
            $data | ? ClusterType -eq SQL | ForEach-Object ClusterIdMin | Should Be 1
            $data | ? ClusterType -eq SQL | ForEach-Object ClusterIdMax | Should Be 6
        }

    }


}

