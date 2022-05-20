Import-Module $PSScriptRoot -Force -DisableNameChecking

Describe 'DB Module' {
    Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
    
    Function CleanQuery($Query) { $Query.Trim() -replace "[`r`n ]+", " " }

    try { Invoke-DBQuery DBTest "ALTER TABLE Tests.Temporal1 SET (SYSTEM_VERSIONING = OFF)" -ErrorAction Stop } catch { }

    Get-DBTable DBTest -TableType Table |
        ForEach-Object { Remove-DBTable DBTest -Schema $_.Schema -Table $_.Table -Confirm:$false }

    Get-DBTable DBTest -TableType View |
        ForEach-Object { Remove-DBView DBTest -Schema $_.Schema -View $_.Table -Confirm:$false }

    try { Remove-DBSchema DBTest -Schema Tests -Confirm:$false -ErrorAction Stop } catch { }
    try { Remove-DBSchema DBTest -Schema TableHerring -Confirm:$false -ErrorAction Stop } catch { }

    Context 'Connection' {
        It 'ConnectionTimeout' {
            Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests -ConnectionTimeout 99
            $connection = $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections['DBTest']
            $connection.ConnectionString | Should Match "Connection Timeout=99;"
        }
            
        It 'Reuses' {
            Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
            $connection1 = $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections['DBTest']
            Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
            $connection2 = $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections['DBTest']
            $connection1.ConnectionObject -eq $connection2.ConnectionObject | Should Be $true
        }

        It 'Close-DBConnection' {
            
            Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
            $connection = $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections['DBTest']
            $connection.ConnectionObject.State | Should Be 'Open'

            Close-DBConnection DBTest

            $connection.ConnectionObject.State | Should Be 'Closed'

            Initialize-DBConnectionToLocalDB DBTest -FilePath C:\Temp\DBTest.mdf -DefaultSchema Tests
        }
    }

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
            New-DBView DBTest -View View1 -SQL "SELECT * FROM Test1" -Force -Confirm:$false

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

        New-DBTable DBTest -Table ClusterType -Definition {
            Define-DBColumn ClusterType nvarchar -Required -Length 32 -PrimaryKey
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

            "ClusterType", "File", "SQL", "Service" | ConvertFrom-Csv | Add-DBRow DBTest -Table ClusterType
        }

        It 'Add-DBRow with Identity' {

            New-DBTable DBTest -Table Identity1 -Definition {
                Define-DBColumn Id int -Required -PrimaryKey -Identity
                Define-DBColumn Value1 nvarchar
            }

            @(
                [pscustomobject]@{Value1='Item1'}
                [pscustomobject]@{Value1='Item2'}
            ) | Add-DBRow DBTest -Table Identity1

            $data = Get-DBRow DBTest -Table Identity1
            $data[0].Id | Should Be 1
            $data[0].Value1 | Should Be Item1

            $data[1].Id | Should Be 2
            $data[1].Value1 | Should Be Item2
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

        It 'Get-DBRow -FilterNullOrEmpty' {
            New-DBTable DBTest -Table FilterNullOrEmptyTest -Definition {
                Define-DBColumn Key nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn Value nvarchar
            }

            [pscustomobject]@{Key='Empty'; Value=''} | Add-DBRow DBTest -Table FilterNullOrEmptyTest
            [pscustomobject]@{Key='Null'} | Add-DBRow DBTest -Table FilterNullOrEmptyTest
            [pscustomobject]@{Key='NotNull'; Value='a'} | Add-DBRow DBTest -Table FilterNullOrEmptyTest

            $debug = Get-DBRow DBTest -Table FilterNullOrEmptyTest -FilterNullOrEmpty Value -DebugOnly
            $debug.Query | Should Be "SELECT * FROM [Tests].[FilterNullOrEmptyTest] T1 WHERE (T1.[Value] IS NULL OR T1.[Value] = '')"

            $values = Get-DBRow DBTest -Table FilterNullOrEmptyTest -FilterNullOrEmpty Value |
                Sort-Object Key |
                ForEach-Object Key
            @($values).Count | Should Be 2
            $values[0] | Should Be 'Empty'
            $values[1] | Should Be 'Null'
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

        It "Joins supports -CountColumnAs (Syntax Check)" {
            $query = Get-DBRow DBTest -Table SourceA -Column ColA -Unique -DebugOnly -Joins {
                Define-DBJoin -RightTable JoinB -RightKey ColA -CountColumnAs @{CountThis='AsThat'}
            }

            $query.Query | Should Be (CleanQuery "
                SELECT T1.[ColA] [ColA], COUNT(T2.[CountThis]) [AsThat]
                FROM [Tests].[SourceA] T1
                    LEFT JOIN [Tests].[JoinB] T2 ON T1.[ColA] = T2.[ColA]
                GROUP BY T1.[ColA]
            ")
        }

        It "Joins supports -CountColumnAs (Reality Check)" {
            $result = Get-DBRow DBTest -Table ClusterType -Column ClusterType -Unique -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterType -CountColumnAs @{ClusterName='Clusters'}
            }

            $result | Where-Object ClusterType -eq SQL | ForEach-Object Clusters | Should Be 3
            $result | Where-Object ClusterType -eq File | ForEach-Object Clusters | Should Be 2
            $result | Where-Object ClusterType -eq Service | ForEach-Object Clusters | Should Be 0
        }

        It "Joins supports -CastNullAsBit (Syntax Check)" {
            $query = Get-DBRow DBTest -Table SourceA -Column ColA -DebugOnly -Joins {
                Define-DBJoin -RightTable JoinB -RightKey ColA -CastNullAsBit @{NullableColumn='AsBitName'}
            }

            $query.Query | Should Be (CleanQuery "
                SELECT T1.[ColA] [ColA], CAST(IIF(T2.[NullableColumn] IS NULL, 0, 1) AS bit) [AsBitName]
                FROM [Tests].[SourceA] T1
                    LEFT JOIN [Tests].[JoinB] T2 ON T1.[ColA] = T2.[ColA]
            ")
        }

        It "Join supports -CastNullAsBit (Reality Check)" {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName, ClusterType -Joins {
                Define-DBJoin DBTest -RightTable Cluster -RightKey ClusterId -JoinFilterEq @{ClusterType='SQL'} -CastNullAsBit @{ClusterId='IsSql'}
            }

            $data | Where-Object ClusterType -eq SQL | ForEach-Object IsSql | Should Be $true
            $data | Where-Object ClusterType -ne SQL | ForEach-Object IsSql | Should Be $false
        }

        It 'Supports Top (Syntax Check)' {
            $query = Get-DBRow DBTest -Table SourceA -Column ColA -OrderBy ColB -Top 2 -DebugOnly
            $query.Query | Should Be (CleanQuery "
                SELECT TOP 2 T1.[ColA] [ColA]
                FROM [Tests].[SourceA] T1
                ORDER BY T1.[ColB]
            ")
        }

        It 'Supports Top (Reality Check)' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterId, ClusterName -OrderBy ClusterName -Top 2
            @($data).Count | Should Be 2
            $data[0].ClusterName | Should Be 'CAFile'
            $data[1].ClusterName | Should Be 'SQL001'
        }

        It 'Get-DBRow -Unique -Count -Min -Max -OrderBy -Joins (No Exception Only)' {
            # Must make sure it doesn't throw an exception
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterType, ClusterName -Unique -Count -Min ClusterId -Max ClusterId -Sum ClusterId -OrderBy ClusterName -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId
            }
            $data[0].ClusterName | Should Match ".+"
        }
    }

    Context 'Default Column Value' {

        It "Functions in New-DBTable > Define-DBColumn (Syntax Validation)" {
            $query = New-DBTable DBTest -Table DefaultNew -DebugOnly -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Default_nvarchar -Type nvarchar -Default abc
                Define-DBColumn Default_int -Type int -Default 99
                Define-DBColumn Default_nvarchar_empty -Type nvarchar -Required -Default ''
            }

            $query.Query -replace "[`r`n ]+", " " | Should Be (CleanQuery "
                CREATE TABLE [Tests].[DefaultNew]
                (
                    [Key] int NOT NULL,
                    [Default_nvarchar] nvarchar(MAX) NULL DEFAULT 'abc',
                    [Default_int] int NULL DEFAULT 99,
                    [Default_nvarchar_empty] nvarchar(MAX) NOT NULL DEFAULT '',
                    CONSTRAINT [PK_DefaultNew] PRIMARY KEY ([Key])
                )
            ")
        }

        It "Functions in New-DBTable > Define-DBColumn (Reality Check)" {
            New-DBTable DBTest -Table DefaultNew -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Default_nvarchar -Type nvarchar -Default abc
                Define-DBColumn Default_int -Type int -Default 99
                Define-DBColumn Default_nvarchar_empty -Type nvarchar -Required -Default ''
            }

            Invoke-DBQuery DBTest "INSERT INTO Tests.DefaultNew ([Key]) VALUES (1)"

            $result = Get-DBRow DBTest -Table DefaultNew -FilterEq @{Key=1}
            $result.Key | Should Be 1
            $result.Default_nvarchar | Should Be 'abc'
            $result.Default_int | Should Be 99
            $result.Default_nvarchar_empty | Should Be ''
        }

        It "Add-DBRow regular mode handles the full suite" {
            [pscustomobject]@{Key=2} | Add-DBRow DBTest -Table DefaultNew

            $result = Get-DBRow DBTest -Table DefaultNew -FilterEq @{Key=2}
            $result.Key | Should Be 2
            $result.Default_nvarchar | Should Be 'abc'
            $result.Default_int | Should Be 99
            $result.Default_nvarchar_empty | Should Be ''
        }

        It "Add-DBRow BulkCopy mode handles the full suite" {
            [pscustomobject]@{Key=3} | Add-DBRow DBTest -Table DefaultNew -BulkCopy

            $result = Get-DBRow DBTest -Table DefaultNew -FilterEq @{Key=3}
            $result.Key | Should Be 3
            $result.Default_nvarchar | Should Be 'abc'
            $result.Default_int | Should Be 99
            $result.Default_nvarchar_empty | Should Be ''
        }

        It "New-DBColumn handles all cases (Syntax Validation)" {
            $query1 = New-DBColumn DBTest -Table Table1 -Column Default_nvarchar -Type nvarchar -Default 'abc' -DebugOnly
            $query1.Query | Should Be "ALTER TABLE [Tests].[Table1] ADD [Default_nvarchar] nvarchar(MAX) NULL DEFAULT 'abc'"
            
            $query2 = New-DBColumn DBTest -Table Table1 -Column Default_int -Type int -Default 99 -DebugOnly
            $query2.Query | Should Be "ALTER TABLE [Tests].[Table1] ADD [Default_int] int NULL DEFAULT 99"
            
            $query3 = New-DBColumn DBTest -Table Table1 -Column Default_nvarchar_empty -Type nvarchar -Required -Default '' -DebugOnly
            $query3.Query | Should Be "ALTER TABLE [Tests].[Table1] ADD [Default_nvarchar_empty] nvarchar(MAX) NOT NULL DEFAULT ''"
        }

        It "New-DBColumn handles all cases (Reality Check)" {
            
            New-DBTable DBTest -Table DefaultAdd -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
            }

            Invoke-DBQuery DBTest "INSERT INTO Tests.DefaultAdd ([Key]) VALUES (1)"

            New-DBColumn DBTest -Table DefaultAdd -Column Default_nvarchar -Type nvarchar -Default 'abc'
            New-DBColumn DBTest -Table DefaultAdd -Column Default_int -Type int -Default 99
            New-DBColumn DBTest -Table DefaultAdd -Column Default_nvarchar_empty -Type nvarchar -Required -Default ''

            Invoke-DBQuery DBTest "INSERT INTO Tests.DefaultAdd ([Key]) VALUES (2)"

            $result1 = Get-DBRow DBTest -Table DefaultAdd -FilterEq @{Key=1}
            $result1.Default_nvarchar | Should Be $null
            $result1.Default_int | Should Be $null
            $result1.Default_nvarchar_empty | Should Be ''

            $result2 = Get-DBRow DBTest -Table DefaultAdd -FilterEq @{Key=2}
            $result2.Default_nvarchar | Should Be 'abc'
            $result2.Default_int | Should Be 99
            $result2.Default_nvarchar_empty | Should Be ''
        }

        It "Update-DBColumn handles all cases (Syntax Validation)" {
            $query1 = Update-DBColumn DBTest -Table Table1 -Column Default_nvarchar -Type nvarchar -Default 'abc' -DebugOnly
            $query1.Query | Should Be (CleanQuery "
                BEGIN
                ALTER TABLE [Tests].[Table1] ALTER COLUMN [Default_nvarchar] nvarchar(MAX) NULL;
                ALTER TABLE [Tests].[Table1] ADD CONSTRAINT [DF_Tests_Table1_Default_nvarchar] DEFAULT 'abc' FOR [Default_nvarchar];
                END;
            ")
            
            $query2 = Update-DBColumn DBTest -Table Table1 -Column Default_int -Type int -Default 99 -DebugOnly
            $query2.Query | Should Be (CleanQuery "
                BEGIN
                ALTER TABLE [Tests].[Table1] ALTER COLUMN [Default_int] int NULL;
                ALTER TABLE [Tests].[Table1] ADD CONSTRAINT [DF_Tests_Table1_Default_int] DEFAULT 99 FOR [Default_int];
                END;
            ")

            $query3 = Update-DBColumn DBTest -Table Table1 -Column Default_nvarchar_empty -Type nvarchar -Required -Default '' -DebugOnly
            $query3.Query | Should Be (CleanQuery "
                BEGIN
                ALTER TABLE [Tests].[Table1] ALTER COLUMN [Default_nvarchar_empty] nvarchar(MAX) NOT NULL;
                ALTER TABLE [Tests].[Table1] ADD CONSTRAINT [DF_Tests_Table1_Default_nvarchar_empty] DEFAULT '' FOR [Default_nvarchar_empty];
                END;
            ")
        }

        It "Update-DBColumn handles all cases (Reality Check)" {
            New-DBTable DBTest -Table DefaultUpdate -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Default_nvarchar -Type nvarchar
                Define-DBColumn Default_int -Type int
                Define-DBColumn Default_nvarchar_empty -Type nvarchar
            }

            # We have to provide a value for Default_nvarchar_empty since the DEFAULT update happens after the column is made NOT NULL
            Invoke-DBQuery DBTest "INSERT INTO Tests.DefaultUpdate ([Key], Default_nvarchar_empty) VALUES (31, '')"

            Update-DBColumn DBTest -Table DefaultUpdate -Column Default_nvarchar -Type nvarchar -Default 'abc'
            Update-DBColumn DBTest -Table DefaultUpdate -Column Default_int -Type int -Default 99
            Update-DBColumn DBTest -Table DefaultUpdate -Column Default_nvarchar_empty -Type nvarchar -Required -Default ''

            Invoke-DBQuery DBTest "INSERT INTO Tests.DefaultUpdate ([Key]) VALUES (32)"

            $result1 = Get-DBRow DBTest -Table DefaultUpdate -FilterEq @{Key=31}
            $result1.Default_nvarchar | Should Be $null
            $result1.Default_int | Should Be $null
            $result1.Default_nvarchar_empty | Should Be ''

            $result2 = Get-DBRow DBTest -Table DefaultUpdate -FilterEq @{Key=32}
            $result2.Default_nvarchar | Should Be 'abc'
            $result2.Default_int | Should Be 99
            $result2.Default_nvarchar_empty | Should Be ''
        }
    }

    Context 'Define-DBJoin with -JoinFilter*' {
        It 'Get-DBRow Join with JoinFilterEq' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -JoinFilterEq @{ClusterType='SQL'}
            }

            $data[0].ClusterName | Should Be SQL001
            $data[0].ClusterType | Should Be SQL
            $data[2].ClusterType | Should Be $null
        }

        It 'Get-DBRow Join with JoinFilterNe' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -JoinFilterNe @{ClusterType='SQL'}
            }

            $data[0].ClusterName | Should Be SQL001
            $data[0].ClusterType | Should Be $null
            $data[2].ClusterType | Should Be File
        }

        It 'Get-DBRow Join with JoinFilterExists' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -JoinFilterExists ([pscustomobject]@{ClusterName='SQL001';ClusterType='SQL'})
            }

            $data[0].ClusterName | Should Be SQL001
            $data[0].ClusterType | Should Be SQL
            $data[1].ClusterType | Should Be $null
            $data[2].ClusterType | Should Be $null
        }

        It 'Get-DBRow Join with Filter + JoinFilterEq' {
            $data = Get-DBRow DBTest -Table Cluster -Column ClusterName -OrderBy ClusterId -FilterGt @{ClusterId=1} -Joins {
                Define-DBJoin -RightTable Cluster -RightKey ClusterId -Column ClusterType -JoinFilterEq @{ClusterType='SQL'}
            }

            $data[0].ClusterName | Should Be SQL002
            $data[0].ClusterType | Should Be SQL
            $data[1].ClusterName | Should Be CAFile
            $data[1].ClusterType | Should Be $null
        }
    }


    Context 'Sync-DBRow' {

        It 'Basic Insert/Update/Delete Test' {

            New-DBTable DBTest -Table SyncRowBasic -Definition {
                Define-DBColumn ComputerName nvarchar -Length 64 -Required -PrimaryKey
                Define-DBColumn OperatingSystem nvarchar
            }

            [pscustomobject]@{
                ComputerName = 'SimpleServer'
                OperatingSystem = 'Server 2016'
            } | Sync-DBRow DBTest -Table SyncRowBasic -BetaAcknowledgement

            $result1 = Get-DBRow DBTest -Table SyncRowBasic -FilterEq @{ComputerName='SimpleServer'}
            $result1.ComputerName | Should Be 'SimpleServer'
            $result1.OperatingSystem | Should Be 'Server 2016'

            [pscustomobject]@{
                ComputerName = 'SimpleServer'
                OperatingSystem = 'Server 2019'
            } | Sync-DBRow DBTest -Table SyncRowBasic -BetaAcknowledgement

            $result2 = Get-DBRow DBTest -Table SyncRowBasic -FilterEq @{ComputerName='SimpleServer'}
            $result2.ComputerName | Should Be 'SimpleServer'
            $result2.OperatingSystem | Should Be 'Server 2019'

            [pscustomobject]@{
                ComputerName = 'NewServer'
                OperatingSystem = 'Server 2022'
            } | Sync-DBRow DBTest -Table SyncRowBasic -BetaAcknowledgement

            Get-DBRow DBTest -Table SyncRowBasic

            $result3 = Get-DBRow DBTest -Table SyncRowBasic -FilterEq @{ComputerName='SimpleServer'}
            $result3 | Should Be $null

            $result4 = Get-DBRow DBTest -Table SyncRowBasic -FilterEq @{ComputerName='NewServer'}
            $result4.ComputerName | Should Be 'NewServer'
            $result4.OperatingSystem | Should Be 'Server 2022'
        }

        It 'Set Insert/Update/Delete Test' {

            New-DBTable DBTest -Table SyncRowSet -Definition {
                Define-DBColumn ComputerName nvarchar -Length 64 -Required -PrimaryKey
                Define-DBColumn OperatingSystem nvarchar
            }

            @(
                [pscustomobject]@{
                    ComputerName = 'WindowsSetServer'
                    OperatingSystem = 'Server 2016'
                }
                [pscustomobject]@{
                    ComputerName = 'LinuxSetServer'
                    OperatingSystem = 'Debian'
                }
            ) | Sync-DBRow DBTest -Table SyncRowSet -SetKeys ComputerName -BetaAcknowledgement

            $result1 = Get-DBRow DBTest -Table SyncRowSet -OrderBy ComputerName
            @($result1).Count | Should Be 2
            $result1[0].ComputerName | Should Be 'LinuxSetServer'
            $result1[0].OperatingSystem | Should Be 'Debian'
            $result1[1].ComputerName | Should Be 'WindowsSetServer'
            $result1[1].OperatingSystem | Should Be 'Server 2016'

            [pscustomobject]@{
                ComputerName = 'WindowsSetServer'
                OperatingSystem = 'Server 2019'
            } | Sync-DBRow DBTest -Table SyncRowSet -SetKeys ComputerName -BetaAcknowledgement

            $result2 = Get-DBRow DBTest -Table SyncRowSet -OrderBy ComputerName
            @($result2).Count | Should Be 2
            $result2[0].ComputerName | Should Be 'LinuxSetServer'
            $result2[0].OperatingSystem | Should Be 'Debian'
            $result2[1].ComputerName | Should Be 'WindowsSetServer'
            $result2[1].OperatingSystem | Should Be 'Server 2019'

            [pscustomobject]@{
                ComputerName = 'LinuxSetServer'
                OperatingSystem = 'RedHat'
            } | Sync-DBRow DBTest -Table SyncRowSet -SetKeys ComputerName -SetValues 'LinuxSetServer' -BetaAcknowledgement

            $result2 = Get-DBRow DBTest -Table SyncRowSet -OrderBy ComputerName
            @($result2).Count | Should Be 2
            $result2[0].ComputerName | Should Be 'LinuxSetServer'
            $result2[0].OperatingSystem | Should Be 'RedHat'
            $result2[1].ComputerName | Should Be 'WindowsSetServer'
            $result2[1].OperatingSystem | Should Be 'Server 2019'
        }

        It 'Bulk Tests - Insert' {
            New-DBTable DBTest -Table SyncBulkTest -Definition {
                Define-DBColumn Index int -Required -PrimaryKey
                Define-DBColumn Mod int -Required
                Define-DBColumn Text nvarchar -Required
            }

            # Insert Many
            $result1 = 1..20 | % {
                [pscustomobject]@{
                    Index = $_
                    Mod = $_ % 3
                    Text = $_.ToString()
                }
            } |
                Sync-DBRow DBTest -Table SyncBulkTest -BetaAcknowledgement

            $result1.CountInput | Should Be 20
            $result1.CountInserted | Should Be 20
            $result1.CountUpdated | Should Be 0
            $result1.CountDeleted | Should Be 0
            $result1.CountNoChange | Should Be 0
        }

        It 'Bulk Tests - No Update Many' {

            $result1 = 1..20 | % {
                [pscustomobject]@{
                    Index = $_
                    Mod = $_ % 3
                    Text = $_.ToString()
                }
            } |
                Sync-DBRow DBTest -Table SyncBulkTest -BetaAcknowledgement

            $result1.CountInput | Should Be 20
            $result1.CountInserted | Should Be 0
            $result1.CountUpdated | Should Be 0
            $result1.CountDeleted | Should Be 0
            $result1.CountNoChange | Should Be 20
        }

        It 'Bulk Tests - Update Many' {

            $result1 = 1..20 | % {
                [pscustomobject]@{
                    Index = $_
                    Mod = $_ % 4
                    Text = $_.ToString()
                }
            } |
                Sync-DBRow DBTest -Table SyncBulkTest -BetaAcknowledgement

            $result1.CountInput | Should Be 20
            $result1.CountInserted | Should Be 0
            $result1.CountUpdated | Should Be 15
            $result1.CountDeleted | Should Be 0
            $result1.CountNoChange | Should Be 5

        }

        It 'Bulk Tests - Update Subset of Many' {

            $result1 = 1..20 | % {
                [pscustomobject]@{
                    Index = $_
                    Mod = $_ % 4
                    Text = "New $_"
                }
            } |
                Where-Object Mod -eq 0 |
                Sync-DBRow DBTest -Table SyncBulkTest -BetaAcknowledgement -SetKeys Mod -SetValues 0

            $result1.CountInput | Should Be 5
            $result1.CountInserted | Should Be 0
            $result1.CountUpdated | Should Be 5
            $result1.CountDeleted | Should Be 0
            $result1.CountNoChange | Should Be 0

        }

        It 'Bulk Tests - Update and Remove Subset of Many' {

            $result1 = 1..20 | % {
                [pscustomobject]@{
                    Index = $_
                    Mod = $_ % 4
                    Text = "New2 $_"
                }
            } |
                Where-Object Index -gt 10 |
                Where-Object Mod -eq 0 |
                Sync-DBRow DBTest -Table SyncBulkTest -BetaAcknowledgement -SetKeys Mod -SetValues 0

            $result1.CountInput | Should Be 3
            $result1.CountInserted | Should Be 0
            $result1.CountUpdated | Should Be 3
            $result1.CountDeleted | Should Be 2
            $result1.CountNoChange | Should Be 0

        }

        It 'Bulk Tests - Remove All' {
            $result1 = @() | Sync-DBRow DBTest -Table SyncBulkTest -BetaAcknowledgement -SetKeys Mod -SetValues 0, 1, 2, 3
            $result1.CountInput | Should Be 0
            $result1.CountInserted | Should Be 0
            $result1.CountUpdated | Should Be 0
            $result1.CountDeleted | Should Be 18
            $result1.CountNoChange | Should Be 0
        }

        It 'Handles key columns only' {
            New-DBTable DBTest -Table SyncKeyOnly -Definition {
                Define-DBColumn ComputerName nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn DomainName nvarchar -Length 32 -Required -PrimaryKey
            }

            $result1 = @(
                [pscustomobject]@{ComputerName='ServerA'; DomainName='DomainA'}
                [pscustomobject]@{ComputerName='ServerA'; DomainName='DomainB'}
            ) | Sync-DBRow DBTest -Table SyncKeyOnly -BetaAcknowledgement

            $result1.CountInserted | Should Be 2

            $result2 = @(
                [pscustomobject]@{ComputerName='ServerA'; DomainName='DomainA'}
                [pscustomobject]@{ComputerName='ServerB'; DomainName='DomainB'}
            ) | Sync-DBRow DBTest -Table SyncKeyOnly -BetaAcknowledgement

            $result2.CountInserted | Should Be 1
            $result2.CountDeleted | Should Be 1
        }

        It 'Handles extra columns' {
            New-DBTable DBTest -Table SyncExtraColumns -Definition {
                Define-DBColumn ComputerName nvarchar -Length 32 -Required -PrimaryKey
            }

            [pscustomobject]@{ComputerName='ServerA'; ExtraField='A'} |
                Sync-DBRow DBTest -Table SyncExtraColumns -BetaAcknowledgement -WarningAction SilentlyContinue
        }

        It 'Handles null values' {
            New-DBTable DBTest -Table SyncNullValues -Definition {
                Define-DBColumn ComputerName nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn PasswordLastSet datetime
            }

            [pscustomobject]@{
                ComputerName = 'ServerA'
                PasswordLastSet = $null
            } | Sync-DBRow DBTest -Table SyncNullValues -BetaAcknowledgement

            [pscustomobject]@{
                ComputerName = 'ServerA'
                PasswordLastSet = $(if ($false) { [datetime]::Today })
            } | Sync-DBRow DBTest -Table SyncNullValues -BetaAcknowledgement
        }
    }

    Context 'Get/Set/Add/Remove Row Edge Cases' {
        It 'Parameter -Timeout exists for all -DBRow cmdlets' {
            Get-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            Get-DBRow DBTest -Table Cluster -FilterEq @{ClusterId=54625654} -Timeout 0 -ErrorAction Stop | Out-Null
            @() | Add-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            @() | Update-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop | Out-Null
            Set-DBRow DBTest -Table Cluster -Timeout 0 -ErrorAction Stop -Set @{ClusterId=0} -FilterEq @{ClusterId=-1} | Out-Null
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

        It 'DBRow with blank pscustomobject' {
            New-DBTable DBTest -Table TestBlankPsCustomObject -Definition {
                Define-DBColumn Index int -Required -PrimaryKey
                Define-DBColumn Object nvarchar
            }

            [pscustomobject]@{
                Index = 1
                Object = if ($false) { 1 }
            } | Add-DBRow DBTest -Table TestBlankPsCustomObject

            $result1 = Get-DBRow DBTest -Table TestBlankPsCustomObject -FilterEq @{Index=1}
            $result1.Object | Should Be $null

            [pscustomobject]@{
                Index = 2
                Object = if ($false) { 1 }
            } | Add-DBRow DBTest -Table TestBlankPsCustomObject -BulkCopy

            $result2 = Get-DBRow DBTest -Table TestBlankPsCustomObject -FilterEq @{Index=2}
            $result2.Object | Should Be $null

            $temp = [pscustomobject]@{Object = if ($false) { 1 } }
            Set-DBRow DBTest -Table TestBlankPsCustomObject -FilterEq @{Index=1} -Set @{Object=$temp.Object}

            $result3 = Get-DBRow DBTest -Table TestBlankPsCustomObject -FilterEq @{Index=1}
            $result3.Object | Should Be $null
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

        It 'Gets Columns By Name' {
            $column = Get-DBColumn DBTest -Table ColumnTest -Column Key2
            $column | Measure-Object | ForEach-Object Count | Should Be 1
            $column[0].Column | Should Be Key2
        }

        It 'Define-DBColumn with a (n)char and no length changes to (n)varchar' {
            $test1 = Define-DBColumn Test1 char -WarningAction SilentlyContinue
            $test1.Type | Should Be varchar
            $test2 = Define-DBColumn Test2 nchar -WarningAction SilentlyContinue
            $test2.Type | Should Be nvarchar
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

    Context 'Foreign Key Constraints' {
        It 'Creates (Syntax Validation)' {
            $query = New-DBForeignKeyConstraint DBTest -Table ForeignChild -Column ParentName -ForeignTable ForeignParent -ForeignColumn ParentName -DebugOnly
            $query.Query | Should Be (CleanQuery "
            ALTER TABLE [Tests].[ForeignChild]
            ADD CONSTRAINT [FK_ForeignChild_ParentName]
            FOREIGN KEY ([ParentName]) REFERENCES [Tests].[ForeignParent] ([ParentName])
            ON UPDATE CASCADE
            ")
        }

        It 'Creates (Reality Check)' {

            New-DBTable DBTest -Table ForeignParent -Definition {
                Define-DBColumn ParentName nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn Description nvarchar
            }

            New-DBTable DBTest -Table ForeignChild -Definition {
                Define-DBColumn ChildName nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn ParentName nvarchar -Length 32 -Required
                Define-DBColumn Description nvarchar
            }

            New-DBForeignKeyConstraint DBTest -Table ForeignChild -Column ParentName -ForeignTable ForeignParent -ForeignColumn ParentName

            [pscustomobject]@{ParentName='A'} | Add-DBRow DBTest -Table ForeignParent
            [pscustomobject]@{ChildName='B'; ParentName='A'} | Add-DBRow DBTest -Table ForeignChild

            $test1 = Get-DBRow DBTest -Table ForeignChild
            $test1.ChildName | Should Be 'B'
            $test1.ParentName | Should Be 'A'

            Set-DBRow DBTest -Table ForeignParent -Set @{ParentName='C'} -FilterEq @{ParentName='A'}

            $test2 = Get-DBRow DBTest -Table ForeignChild
            $test2.ChildName | Should Be 'B'
            $test2.ParentName | Should Be 'C'
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

    Context 'Temporal Tables' {
        It 'Creates temporal tables (Syntax Check)' {
            $query = New-DBTable DBTest -Table Temporal1 -DebugOnly -Definition {
                Define-DBColumn Username nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn FullName nvarchar -Length 256
                Define-DBColumn Weight int
                Define-DBTemporalTableSettings -SysStartTimeColumn ValidFrom -SysEndTimeColumn ValidTo -HistorySchema Tests -HistoryTable Temporal1_History -BetaAcknowledgement
            }

            $query.Query | Should Be ("
            CREATE TABLE [Tests].[Temporal1]
            (
                [Username] nvarchar(32) NOT NULL,
                [FullName] nvarchar(256) NULL,
                [Weight] int NULL,
                [ValidFrom] datetime2 GENERATED ALWAYS AS ROW START,
                [ValidTo] datetime2 GENERATED ALWAYS AS ROW END,
                PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),
                CONSTRAINT [PK_Temporal1] PRIMARY KEY ([Username])
            )
            WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [Tests].[Temporal1_History]))
            " -replace '(?m)^ {12}').Trim()
        }

        It 'Creates temporal tables (Reality Check)' {
            New-DBTable DBTest -Table Temporal1 -Definition {
                Define-DBColumn Username nvarchar -Length 32 -Required -PrimaryKey
                Define-DBColumn FullName nvarchar -Length 256
                Define-DBColumn Weight int
                Define-DBTemporalTableSettings -SysStartTimeColumn ValidFrom -SysEndTimeColumn ValidTo -HistorySchema Tests -HistoryTable Temporal1_History -BetaAcknowledgement
            }

            $mainColList = Get-DBColumn DBTest -Table Temporal1
            $historyColList = Get-DBColumn DBTest -Table Temporal1_History

            $mainColList.Column -join '|' | Should Be "Username|FullName|Weight|ValidFrom|ValidTo"
            $historyColList.Column -join '|' | Should Be "Username|FullName|Weight|ValidFrom|ValidTo"
        }

        It 'Adds to / updates temporal tables' {
            [pscustomobject]@{Username='jsmith'; FullName='John Smith'; Weight=220} |
                Add-DBRow DBTest -Table Temporal1

            $main1 = Get-DBRow DBTest -Table Temporal1
            $history1 = Get-DBRow DBTest -Table Temporal1_History

            @($main1).Count | Should Be 1
            @($history1).Count | Should Be 0

            $main1.Username | Should Be jsmith
            $main1.Weight | Should Be 220

            Set-DBRow DBTest -Table Temporal1 -Set @{Weight=215} -FilterEq @{Username='jsmith'}

            $main2 = Get-DBRow DBTest -Table Temporal1
            $history2 = Get-DBRow DBTest -Table Temporal1_History

            @($main2).Count | Should Be 1
            @($history2).Count | Should Be 1

            $main2.Username | Should Be jsmith
            $main2.Weight | Should Be 215
            $history2.Username | Should Be jsmith
            $history2.Weight | Should Be 220
        }
    }

    Context 'Transactions' {
        It "Can rollback transactions" {
            New-DBTable DBTest -Table Transaction1 -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Value1 nvarchar
            }

            [pscustomobject]@{
                Key = 1
                Value1 = 'Before'
            } | Add-DBRow DBTest -Table Transaction1
            
            Use-DBTransaction DBTest
            
            Set-DBRow DBTest -Table Transaction1 -FilterEq @{Key=1} -Set @{Value1='After'}
            Get-DBRow DBTest -Table Transaction1 -Column Value1 |
                ForEach-Object Value1 |
                Should Be 'After'

            Undo-DBTransaction DBTest

            Get-DBRow DBTest -Table Transaction1 -Column Value1 |
                ForEach-Object Value1 |
                Should Be 'Before'
        }

        It "Can commit transactions" {
            New-DBTable DBTest -Table Transaction2 -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Value1 nvarchar
            }

            [pscustomobject]@{
                Key = 1
                Value1 = 'Before'
            } | Add-DBRow DBTest -Table Transaction2
            
            Use-DBTransaction DBTest
            
            Set-DBRow DBTest -Table Transaction2 -FilterEq @{Key=1} -Set @{Value1='After'}
            Get-DBRow DBTest -Table Transaction2 -Column Value1 |
                ForEach-Object Value1 |
                Should Be 'After'

            Complete-DBTransaction DBTest

            Get-DBRow DBTest -Table Transaction2 -Column Value1 |
                ForEach-Object Value1 |
                Should Be 'After'
        }
    }

    Context 'Edge Cases' {
        
        It 'Uses the right table abbreviations when self-joining' {
            # If we join to ourselves we need to not overwrite out shorthand table
            # or it will cause issues with other joins

            $query = Get-DBRow DBTest -Table Me -Column Me1 -DebugOnly -Joins {
                Define-DBJoin -RightTable Me -RightKey KeyCol1 -Column Me2
                Define-DBJoin -RightTable Other -RightKey KeyCol2 -Column Other1
            }

            $query.Query | Should Be (CleanQuery "
                SELECT T1.[Me1] [Me1], T2.[Me2] [Me2], T3.[Other1] [Other1]
                FROM [Tests].[Me] T1
                    LEFT JOIN [Tests].[Me] T2 ON T1.[KeyCol1] = T2.[KeyCol1]
                    LEFT JOIN [Tests].[Other] T3 ON T1.[KeyCol2] = T3.[KeyCol2]
            ")
        }

        It "Add-DBRow with a timespan column needs whitespace changed to null" {
            New-DBTable DBTest -Table TimespanWhitespace -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Span time
            }

            [pscustomobject]@{Key=1; Span=[TimeSpan]::FromSeconds(1)} | Add-DBRow DBTest -Table TimespanWhitespace
            [pscustomobject]@{Key=2; Span=""} | Add-DBRow DBTest -Table TimespanWhitespace
        }

        It "Add-DBRow with two date time columns adding just one" {
            New-DBTable DBTest -Table TwoDateTimeTest -Definition {
                Define-DBColumn Key int -Required -PrimaryKey
                Define-DBColumn Time1 datetime
                Define-DBColumn Time2 datetime
            }

            [pscustomobject]@{
                Key = 1
                Time1 = [DateTime]::UtcNow
            } | Add-DBRow DBTest -Table TwoDateTimeTest

            [pscustomobject]@{
                Key = 2
                Time2 = [DateTime]::UtcNow
            } | Add-DBRow DBTest -Table TwoDateTimeTest
        }

        It 'Add-DBRow empty column' {
            New-DBTable DBTest -Table NullTest1 -Definition {
                Define-DBColumn ComputerName nvarchar -Length 15 -Required -PrimaryKey
                Define-DBColumn InitializationPages nvarchar
            }

            [pscustomobject]@{
                ComputerName='ABCDEFG'
            } | Add-DBRow DBTest -Table NullTest1
        }
    }

}

