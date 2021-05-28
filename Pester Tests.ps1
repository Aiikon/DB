Import-Module $PSScriptRoot -Force -DisableNameChecking

Describe 'DB Module' {
    Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
    
    Get-DBTable DBTest -TableType Table |
        ForEach-Object { Remove-DBTable DBTest -Schema $_.Schema -Table $_.Table -Confirm:$false }

    Get-DBTable DBTest -TableType View |
        ForEach-Object { Remove-DBView DBTest -Schema $_.Schema -View $_.Table -Confirm:$false }

    try { Remove-DBSchema DBTest -Schema Tests -Confirm:$false -ErrorAction Stop } catch { }
    try { Remove-DBSchema DBTest -Schema TableHerring -Confirm:$false -ErrorAction Stop } catch { }

    Context 'Query' {
        It 'Invoke-DBQuery -Mode Reader' {
            $result1 = Invoke-DBQuery DBTest -Mode Reader "SELECT 'A' Value"
            $result1.Value | Should Be 'A'

            $result2 = Invoke-DBQuery DBTest "SELECT 'A' Value"
            $result2.Value | Should Be 'A'
        }

        It 'Invoke-DBQuery -Mode Scalar' {
            $result = Invoke-DBQuery DBTest -Mode Scalar "SELECT 'A' Value"
            $result | Should Be 'A'
        }

        It 'Invoke-DBQuery -Mode NonQuery' {
            $result = Invoke-DBQuery DBTest -Mode NonQuery "SELECT 'A' Value"
            $result | Should Be -1
        }

        It 'Invoke-DBQuery -Timeout' {
            $message = try
            {
                Invoke-DBQuery DBTest -Mode Scalar "WAITFOR DELAY '00:00:10'; SELECT 'A' Value" -Timeout 1 -ErrorAction Stop | Out-Null
            }
            catch { $_.Exception.Message }

            $message | Should Match 'Execution Timeout Expired'
        }
    }

    Context 'Schemas' {
        
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

    Context 'Table/Views' {
        
        It 'New-DBTable' {

            New-DBTable DBTest -Table Cluster -Definition {
                Define-DBColumn ClusterId int -Required -PrimaryKey
                Define-DBColumn ClusterName nvarchar -Required -Length 15
                Define-DBColumn ClusterType nvarchar
            }

            Get-DBTable DBTest | Where-Object Table -eq Cluster | Should Not BeNullOrEmpty 
        }

        New-DBSchema DBTest -Schema TableHerring

        New-DBTable DBTest -Table Test1 -Definition { Define-DBColumn Id int -Required -PrimaryKey }
        New-DBTable DBTest -Table Test2 -Definition { Define-DBColumn Id int -Required -PrimaryKey }

        New-DBTable DBTest -Schema TableHerring -Table Test1 -Definition { Define-DBColumn Id int -Required -PrimaryKey }
        New-DBTable DBTest -Schema TableHerring -Table Test2 -Definition { Define-DBColumn Id int -Required -PrimaryKey }

        It 'Get-DBTable -Table' {
            $table = Get-DBTable DBTest -Table Test1
            @($table).Count | Should Be 1
            $table.Table | Should Be Test1

            $tableList = Get-DBTable DBTest
            @($tableList).Count | Should BeGreaterThan 1 
        }

        It 'Get-DBTable -Schema' {
            $table = Get-DBTable DBTest -Schema TableHerring
            @($table).Count | Should Be 2
            $table[0].Table | Should Be Test1
            $table[1].Table | Should Be Test2
            $table[0].TableType | Should Be Table
        }

        It 'Get-DBTable -TableType Table' {
            $table = Get-DBTable DBTest -TableType Table
            $table[0].TableType | Should Be Table
        }

        It 'Remove-DBTable' {

            Remove-DBTable DBTest -Table Cluster -Confirm:$false

            Get-DBTable DBTest | Where-Object Table -eq Cluster | Should BeNullOrEmpty 
        }
    }

    Context 'View Creation' {
        It 'New-DBView' {
            New-DBView DBTest -View View1 -SQL "SELECT * FROM Test1"
            New-DBView DBTest -View View1 -SQL "SELECT * FROM Test1" -Force

            $view = Get-DBTable DBTest -Table View1
            $view.Table | Should Be View1
        }

        It 'Get-DBTable -TableType View' {
            $table = Get-DBTable DBTest -TableType View
            $table[0].TableType | Should Be View
        }
        
        It 'Remove-DBView' {
            Remove-DBView DBTest -View View1 -Confirm:$false
        }

        It 'Get-DBViewSql' {
            New-DBView DBTest -View View2 -SQL "SELECT * FROM Test1"
            $view = Get-DBViewSql DBTest -View View2
            $view.SQL | Should Be "CREATE VIEW [Tests].[View2] AS SELECT * FROM Test1"
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

        It 'Get-DBRow -Rename' {
            $data = Get-DBRow DBTest -Table Cluster -FilterEq @{ClusterId=1} -Column ClusterName, ClusterType -Rename @{ClusterType='Type'}
            $data[0].ClusterName | Should Be SQL001
            $data[0].Type | Should Be SQL
        }

        It 'Get-DBRow -Column -Unique -Rename' {
            $results = Get-DBRow DBTest -Table Cluster -Column ClusterType -Unique -Rename @{ClusterType='Type'}

            @($results).Count | Should Be 2
            $results[0].PSObject.Properties.Name | Should Be Type
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

        It 'Get-DBRow -OrderBy' {
            $data = Get-DBRow DBTest -Table Cluster -OrderBy ClusterName
            $data[0].ClusterName | Should Be CAFile
            $data[-1].ClusterName | Should Be TXFile
        }

        It 'Get-DBRow -Join' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType
            }

            $data[0].ClusterName | Should Be SQL001
            $data[0].ClusterType | Should Be SQL
        }

        It 'Get-DBRow -Join with -FilterLike' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -FilterLike @{ClusterName='SQL%'} -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -FilterGe @{ClusterId=4}
            }

            $data | Measure-Object | ForEach-Object Count | Should Be 1
            $data[0].ClusterName | Should Be SQL003
        }

        It 'Get-DBRow -Join -Rename' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -Rename @{ClusterType='Type1'}
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -Rename @{ClusterType='Type2'}
            }

            $data[0].ClusterName | Should Be SQL001
            $data[0].Type1 | Should Be SQL
            $data[0].Type2 | Should Be SQL
        }

        It 'Get-DBRow -Rename BadName!!' {
            $hadException = $false
            try
            {
                Get-DBRow DBTest -Table Cluster -Column ClusterId, ClusterName -Rename @{ClusterName='ClusterName;'}
            }
            catch
            {
                $hadException = $_.Exception.Message -match "Invalid Rename value"
            }

            $hadException | Should Be $true
        }

        It 'Get-DBRow -Join -Rename BadName!!' {
            $hadException = $false
            try
            {
                Get-DBRow DBTest -Table Cluster -Column ClusterId, ClusterName  -Joins {
                    Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -Rename @{ClusterType='Type;'}
                }
            }
            catch
            {
                $hadException = $_.Exception.Message -match "Invalid Rename value"
            }

            $hadException | Should Be $true
        }

        It 'Get-DBRow -Unique -Count -Min -Max -OrderBy -Joins (No Exception Only)' {
            # Must make sure it doesn't throw an exception
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterType, ClusterName -Unique -Count -Min ClusterId -Max ClusterId -Sum ClusterId -OrderBy ClusterName -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId
            }
            $data[0].ClusterName | Should Match ".+"
        }
    }

    Context 'Get/Set/Add/Remove Row Edge Cases' {
        It 'Parameter -Timeout exists for all -DBRow cmdlets' {
            Get-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            Get-DBRow DBTest -Table Cluster -FilterEq @{ClusterId=54625654} -Timeout 0 -ErrorAction Stop | Out-Null
            @() | Add-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            @() | Update-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            Set-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            $true | Should Be $true # Just make sure it accepts the parameter, it's a pain to test
        }

        It 'Set-DBRow with Keys column name' {
            New-DBTable DBTest -Table AddKeysTest -Definition {
                Define-DBColumn Id int -Required -PrimaryKey
                Define-DBColumn Keys nvarchar -Required
            }

            [pscustomobject]@{
                Id = 1
                Keys = 'Test'
            } | Add-DBRow DBTest -Table AddKeysTest

            Set-DBRow DBTest -Table AddKeysTest -Set @{Keys='New'} -FilterEq @{Id=1}
        }

        It 'Add-DBRow positions correctly after a column has been deleted' {
            New-DBTable DBTest -Table AddWithMissing -Definition {
                Define-DBColumn One int -Required -PrimaryKey
                Define-DBColumn Two int
                Define-DBColumn Three int
            }

            Remove-DBColumn DBTest -Table AddWithMissing -Column Two -Confirm:$false

            $colList = Get-DBColumn DBTest -Table AddWithMissing
            $colList[0].Position | Should Be 1
            $colList[1].Position | Should Be 3

            [pscustomobject]@{
                One = 1
                Three = 3
            } | Add-DBRow DBTest -Table AddWithMissing

            $data1 = Get-DBRow DBTest -Table AddWithMissing -FilterEq @{One=1}
            $data1.One | Should Be 1
            $data1.Three | Should Be 3

            [pscustomobject]@{
                One = -1
                Three = -3
            } | Add-DBRow DBTest -Table AddWithMissing -BulkCopy

            $data2 = Get-DBRow DBTest -Table AddWithMissing -FilterEq @{One=-1}
            $data2.One | Should Be -1
            $data2.Three | Should Be -3

            Remove-DBTable DBTest -Table AddWithMissing -Confirm:$false
        }
    }

    Context 'Table Columns' {
        try { Remove-DBTable DBTest -Table ColumnTest -Confirm:$false -ErrorAction Stop } catch { }
        New-DBTable DBTest -Table ColumnTest -Definition {
            Define-DBColumn Key1 int -Required -PrimaryKey
            Define-DBColumn Key2 bigint -Required -PrimaryKey
            Define-DBColumn Value1 nvarchar
        }

        It 'Gets Columns' {
            $column = Get-DBColumn DBTest -Table ColumnTest
            $column | Measure-Object | ForEach-Object Count | Should Be 3
            $column[0].Column | Should Be Key1
            $column[1].Column | Should Be Key2
            $column[2].Column | Should Be Value1

            $column[0].IsPrimaryKey | Should Be $true
            $column[1].IsPrimaryKey | Should Be $true
            $column[2].IsPrimaryKey | Should Be $false

            $column[0].IsNullable | Should Be $false
            $column[1].IsNullable | Should Be $false
            $column[2].IsNullable | Should Be $true
        }
    }

    Context 'Triggers' {
        It 'New-DBTrigger' {
            New-DBTable DBTest -Table TriggerSource -Definition {
                Define-DBColumn Id int -Required -PrimaryKey
            }

            New-DBTable DBTest -Table TriggerDestination -Definition {
                Define-DBColumn Id int -Required -PrimaryKey
            }

            New-DBTrigger DBTest -Table TriggerSource -TriggerFor Insert -Trigger TriggerSource_Insert -SQL "INSERT INTO TriggerDestination (Id) SELECT Id FROM INSERTED"

            [pscustomobject]@{Id=9} | Add-DBRow DBTest -Table TriggerSource
            $data = Get-DBRow DBTest -Table TriggerDestination -FilterEq @{Id=9}
            $data.Id | Should Be 9
        }

        It 'Get-DBTrigger' {
            $trigger1 = Get-DBTrigger DBTest -Table TriggerSource
            $trigger1.Trigger | Should Be 'TriggerSource_Insert'

            $trigger2 = Get-DBTrigger DBTest -Trigger TriggerSource_Insert
            $trigger2.SQL | Should Match "CREATE TRIGGER"
            $trigger2.SQL | Should Match ([Regex]::Escape("INSERT INTO TriggerDestination (Id) SELECT Id FROM INSERTED"))
        }

        It 'Remove-DBTrigger' {
            Remove-DBTrigger DBTest -Trigger TriggerSource_Insert -Confirm:$false

            $trigger = Get-DBTrigger DBTest -Trigger TriggerSource_Insert
            $trigger | Should BeNullOrEmpty
        }
    }

    Context 'Audit Tables' {
        try { Remove-DBTable DBTest -Table AuditTest -Confirm:$false -ErrorAction Stop } catch { }
        try { Remove-DBAuditTable DBTest -Table AuditTest -Confirm:$false -ErrorAction SilentlyContinue } catch { }

        New-DBTable DBTest -Table AuditTest -Definition {
            Define-DBColumn ServiceName nvarchar -Length 32 -Required -PrimaryKey
            Define-DBColumn State nvarchar -Required
            Define-DBColumn Description nvarchar
        }

        It 'Creates Audit Tables' {
            New-DBAuditTable DBTest -Table AuditTest -IncludeAfter
            $table = Get-DBTable DBTest | Where-Object Table -eq AuditTest_Audit
            $table.Table | Should Be AuditTest_Audit
        }

        It 'Audits Inserts' {
            [pscustomobject]@{ServiceName='TestService'; State = 'Running'; Description = 'Sample Service'} |
                Add-DBRow DBTest -Table AuditTest

            $data = Get-DBRow DBTest -Table AuditTest
            $data.ServiceName | Should Be 'TestService'

            $audit = Get-DBRow DBTest -Table AuditTest_Audit | Where-Object __Type -eq 'I'
            $audit.__Type | Should Be 'I'
            $audit.ServiceName | Should Be 'TestService'
            $audit.State__Before | Should Be $null
            $audit.State__Updated | Should Be $null
            $audit.State__After | Should Be 'Running'

            $audit.Description__After | Should Be 'Sample Service'
        }

        It 'Audits Updates' {
            [pscustomobject]@{ServiceName='TestService'; State='Stopped'} | Update-DBRow DBTest -Table AuditTest

            $data = Get-DBRow DBTest -Table AuditTest
            $data.ServiceName | Should Be 'TestService'
            $data.State | Should Be 'Stopped'

            $audit = Get-DBRow DBTest -Table AuditTest_Audit | Where-Object __Type -eq 'U'
            $audit.__Type | Should Be 'U'
            $audit.ServiceName | Should Be 'TestService'
            $audit.State__Updated | Should Be $true
            $audit.State__Before | Should Be 'Running'
            $audit.State__After | Should Be 'Stopped'
            $audit.Description__Updated | Should Be $false
            # $audit.Description__Before | Should Be $null # Unsure
            # $audit.Description__After | Should Be $null # Unsure
        }

        It 'Audits Deletes' {
            Remove-DBRow DBTest -Table AuditTest -Confirm:$false

            $data = Get-DBRow DBTest -Table AuditTest
            @($data).Count | Should Be 0

            $audit = Get-DBRow DBTest -Table AuditTest_Audit | Where-Object __Type -eq 'D'
            $audit.__Type | Should Be 'D'
            $audit.ServiceName | Should Be 'TestService'
            $audit.State__Updated | Should Be $null
            $audit.State__Before | Should Be 'Stopped'
            $audit.State__After | Should Be $null
            $audit.Description__Updated | Should Be $null
            $audit.Description__Before | Should Be 'Sample Service'
            $audit.Description__After | Should Be $null
        }
    }


}

