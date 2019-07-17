if (!$Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections)
{
    $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5 = @{}
    $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections = @{}
}
$Script:ModuleConfig = $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5

Function Initialize-DBConnectionToLocalDB
{
    Param
    (
        [Parameter(Mandatory=$true,Position=0)] [string] $ConnectionName,
        [Parameter(Mandatory=$true)] [string] $FilePath,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $DefaultSchema = 'dbo'
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        try
        {
            $oldConnection = Get-DBConnection $Name
            $oldConnection.Object.Dispose()
        } catch { }

        $firstInstance = sqllocaldb.exe info |
            Where-Object { $_ } |
            Select-Object -First 1
        $connectionString = "Data Source=(LocalDB)\$firstInstance;"
        $connectionObject = New-Object System.Data.SqlClient.SqlConnection

        $FilePath = $PSCmdlet.GetUnresolvedProviderPathFromPSPath($FilePath)
        if (![System.IO.File]::Exists($FilePath))
        {
            $dbName = [System.IO.Path]::GetFileNameWithoutExtension($FilePath)
            if ($dbName -notmatch "\A[A-Za-z0-9 _\-]+\Z")
            {
                throw "Database file name must consist only of letters, numbers, spaces, underscores or dashes"
            }
            $connectionObject.ConnectionString = $connectionString
            $connectionObject.Open()
            $command = $connectionObject.CreateCommand()
            $command.CommandText = "CREATE DATABASE [$dbName] ON (NAME='$dbName', FILENAME='$FilePath')"
            $command.ExecuteScalar()
            $connectionObject.Close()
        }

        $connectionString = "$connectionString;AttachDbFilename=$FilePath;"
        $connectionObject.ConnectionString = $connectionString
        $connectionObject.Open()

        $connection = 1 | Select-Object Name, Type, DefaultSchema, ConnectionObject, Transaction
        $connection.Name = $ConnectionName
        $Connection.Type = "LocalDB"
        $connection.DefaultSchema = $DefaultSchema
        $connection.ConnectionObject = $connectionObject

        $Script:ModuleConfig.Connections[$ConnectionName] = $connection
    }
}

Function Connect-DBConnection
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Position=1)] [string] $Schema
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = $Script:ModuleConfig.Connections[$Connection]
        if (!$dbConnection) { throw "Connection '$Connection' is not defined." }
        if ($dbConnection.ConnectionObject.State -ne 'Open')
        {
            $dbConnection.ConnectionObject.Open()
        }
        $dbConnection
        if ($PSBoundParameters.ContainsKey('Schema'))
        {
            if ($Schema) { return $Schema }
            return $dbConnection.DefaultSchema
        }
    }
}

Function Use-DBTransaction
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection
    )
    End
    {
        $dbConnection = Connect-DBConnection $Connection
        Write-Verbose "Starting transaction on $Connection"
        $dbConnection.Transaction = $dbConnection.ConnectionObject.BeginTransaction()
    }
}

Function Complete-DBTransaction
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection
        $transaction = $dbConnection.Transaction
        if (!$transaction) { throw "Connection does not have an active transaction." }
        Write-Verbose "Completing transaction on $Connection"
        $transaction.Commit()
        $transaction.Dispose()
        $dbConnection.Transaction = $null
    }
}

Function Undo-DBTransaction
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection
        $transaction = $dbConnection.Transaction
        if (!$transaction) { throw "Connection does not have an active transaction." }
        Write-Verbose "Cancelling transaction on $Connection"
        $transaction.Rollback()
        $transaction.Dispose()
        $dbConnection.Transaction = $null
    }
}

Function Invoke-DBQuery
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true, Position=1)] [string] $Query,
        [Parameter()] [hashtable] $Parameters = @{},
        [Parameter()] [string] [ValidateSet('Reader', 'NonQuery', 'Scalar')] $Mode = 'Reader'
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection

        $command = $dbConnection.ConnectionObject.CreateCommand()
        $command.CommandText = $Query
        if ($dbConnection.Transaction) { $command.Transaction = $dbConnection.Transaction }
        $exception = $null

        "Final Query:", $Query | Write-Verbose

        foreach ($parameter in $Parameters.Keys)
        {
            $value = $Parameters[$parameter]
            [void]$command.Parameters.Add($parameter, $value)
        }

        try
        {
            if ($Mode -eq 'NonQuery')
            {
                $command.ExecuteNonQuery()
                $command.Dispose()
                return
            }
            elseif ($Mode -eq 'Scalar')
            {
                $result = $command.ExecuteScalar()
                $command.Dispose()
                if ($result -eq [System.DBNull]::Value) { $result = $null }
                return $result
            }

            $reader = $command.ExecuteReader()

            $propertyList = $reader.GetSchemaTable()

            while ($reader.Read())
            {
                $result = [ordered]@{}
                foreach ($property in $propertyList)
                {
                    $propertyName = $property.ColumnName
                    $value = $reader[$propertyName]
                    if ([System.DBNull]::Value.Equals($value)) { $value = $null }
                    $result.$propertyName = $value
                }
                [pscustomobject]$result
            }
        }
        catch
        {
            $exception = $_
        }
        finally
        {
            trap { continue }
            $command.Cancel()
            $reader.Close()
            $command.Dispose()
        }

        if ($exception) { throw $exception }
    }
}

Function Get-DBDatabase
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection
        $dbConnection.ConnectionObject.GetSchema('Databases')
    }
}

Function New-DBDatabase
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Database,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-\\\.\:]+\Z")] [string] $FileName
    )
    End
    {
        $query = "CREATE DATABASE [$Database] ON (NAME='$Database', FILENAME='$FileName')"

        Invoke-DBQuery $Connection $query -Mode Scalar
    }
}

Function Remove-DBDatabase
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Database
    )
    End
    {
        $query = "DROP DATABASE [$Database]"

        if ($PSCmdlet.ShouldProcess($Database, 'Drop Database'))
        {
            Invoke-DBQuery $Connection $query -Mode Scalar
        }
    }
}

Function Get-DBSchema
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        Invoke-DBQuery $Connection "SELECT name [Schema], schema_id SchemaId FROM sys.schemas"
    }
}

Function New-DBSchema
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        $query = "CREATE SCHEMA [$Schema]"

        Invoke-DBQuery $Connection $query -Mode Scalar
    }
}

Function Remove-DBSchema
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        $query = "DROP SCHEMA [$Schema]"

        if ($PSCmdlet.ShouldProcess($Schema, 'Drop Schema'))
        {
            Invoke-DBQuery $Connection $query -Mode Scalar
        }
    }
}

Function Get-DBTable
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection
        $dbConnection.ConnectionObject.GetSchema('Tables') |
            ForEach-Object {
                $temp = [ordered]@{}
                $temp.Database = $_.TABLE_CATALOG
                $temp.Schema = $_.TABLE_SCHEMA
                $temp.Table = $_.TABLE_NAME
                $temp.TableType = $_.TABLE_TYPE
                [pscustomobject]$temp
            } |
            Sort-Object Database, Schema, Table
    }
}

Function New-DBTable
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [scriptblock] $Definition
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        
        $definitionList = $Definition.Invoke()
        $columnDefinitionList = $definitionList | Where-Object DefinitionType -eq Column
        if (!$columnDefinitionList) { throw "At least one column must be provided" }
        
        $primaryKeyDefinition = $definitionList | Where-Object DefinitionType -eq PrimaryKey
        if (@($primaryKeyDefinition).Count -gt 1) { throw "Only one Define-DBPrimaryKey statement can be provided." }
        $primaryKeyColumnList = $columnDefinitionList | Where-Object PrimaryKey
        if ($primaryKeyDefinition -and $primaryKeyColumnList)
        {
            throw "PrimaryKey cannot be specified both on Define-DBColumn and with Define-DBPrimaryKey."
        }
        if ($primaryKeyColumnList)
        {
            $primaryKeyDefinition = Define-DBPrimaryKey -Column $primaryKeyColumnList.Name
        }

        if (!$primaryKeyDefinition) { Write-Warning "No primary key was specified for $Schema.$Table!" }

        $tableSql = New-Object System.Collections.Generic.List[string]
        $tableSql.Add("CREATE TABLE [$Schema].[$Table]")
        $tableSql.Add("(")

        $definitionSqlList = New-Object System.Collections.Generic.List[string]

        foreach ($columnDefinition in $columnDefinitionList)
        {
            $columnName = $columnDefinition.Name
            $columnSql = "    " + (Get-DBColumnSql $columnDefinition.Name $columnDefinition.Type -Length $columnDefinition.Length -Required:$columnDefinition.Required)
            $definitionSqlList.Add($columnSql)
            if ($columnDefinition.Unique)
            {
                $definitionSqlList.Add("    CONSTRAINT [AK_$columnName] UNIQUE ([$columnName])")
            }
            elseif ($columnDefinition.Index)
            {
                $definitionSqlList.Add("    INDEX [IX_$columnName] ([$columnName])")
            }
        }

        if ($primaryKeyDefinition)
        {
            $primaryKeyName = $primaryKeyDefinition.Name
            if (!$primaryKeyName) { $primaryKeyName = "PK_${Table}" }
            $columnNameSql = $(foreach ($column in $primaryKeyDefinition.Column) { "[$column]" }) -join ','
            $definitionSqlList.Add("    CONSTRAINT [$primaryKeyName] PRIMARY KEY ($columnNameSql)")
        }

        $columnDefinitionList |
            Where-Object UniqueIndexName |
            ForEach-Object { foreach ($index in $_.UniqueIndexName) { [pscustomobject]@{Index=$index; Column=$_.Name} } } |
            Group-Object Index |
            ForEach-Object {
                $columnNameSql = $(foreach ($column in $_.Group.Column) { "[$column]" }) -join ','
                $definitionSqlList.Add("    CONSTRAINT [$($_.Name)] UNIQUE ($columnNameSql)")
            }

        $columnDefinitionList |
            Where-Object IndexName |
            ForEach-Object { foreach ($index in $_.IndexName) { [pscustomobject]@{Index=$index; Column=$_.Name} } } |
            Group-Object Index |
            ForEach-Object {
                $columnNameSql = $(foreach ($column in $_.Group.Column) { "[$column]" }) -join ','
                $definitionSqlList.Add("    INDEX [$($_.Name)] ($columnNameSql)")
            }

        $tableSql.Add($definitionSqlList -join ",`r`n")
        $tableSql.Add(")")

        Invoke-DBQuery $Connection ($tableSql -join "`r`n") -Mode NonQuery | Out-Null
    }
}

Function Remove-DBTable
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        if ($PSCmdlet.ShouldProcess("$Schema.$Table", 'Drop Table'))
        {
            Invoke-DBQuery $Connection "DROP TABLE [$Schema].[$Table]" -Mode NonQuery | Out-Null
        }
    }
}

Function Get-DBView
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection
        $dbConnection.ConnectionObject.GetSchema('Views')
    }
}

Function New-DBView
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $View,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [string] $SQL
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        Invoke-DBQuery $Connection "CREATE VIEW [$Schema].[$View] AS $SQL"
    }
}

Function Remove-DBView
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $View,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        if ($PSCmdlet.ShouldProcess("$Schema.$View", 'Drop View'))
        {
            Invoke-DBQuery $Connection "DROP VIEW [$Schema].[$View]" -Mode NonQuery | Out-Null
        }
    }
}

Function Get-DBWhereSql
{
    [CmdletBinding()]
    Param()
    End
    {
        $opDict = [ordered]@{}
        $opDict.Eq = '='
        $opDict.Ne = '!='
        $opDict.Gt = '>'
        $opDict.Ge = '>='
        $opDict.Lt = '<'
        $opDict.Le = '<='
        $opDict.Like = 'LIKE'
        $opDict.NotLike = 'NOT LIKE'

        $otherDict = [ordered]@{}
        $otherDict.Null = "IS NULL"
        $otherDict.NotNull = "IS NOT NULL"

        $parameterDict = @{}

        $whereList = New-Object System.Collections.Generic.List[string]
        
        $p = 0
        foreach ($op in $opDict.Keys)
        {
            $filterDict = $PSCmdlet.SessionState.PSVariable.GetValue("Filter$op")
            if (!$filterDict) { continue }
            $op2 = $opDict[$op]
            foreach ($col in $filterDict.Keys)
            {
                $value = $filterDict.$col
                if ($value -eq $null)
                {
                    throw "Filter$op[$col] was null and can't be used."
                }

                if ($value -is [array])
                {
                    if ($op -in 'Eq', 'Ne')
                    {
                        $temp = foreach ($newValue in $value)
                        {
                            "@P$p"
                            $parameterDict["P$p"] = $newValue
                            $p += 1
                        }
                        if ($op -eq 'Eq')
                        {
                            $whereList.Add("[$col] IN ($($temp -join ','))")
                        }
                        else
                        {
                            $whereList.Add("[$col] NOT IN ($($temp -join ','))")
                        }
                    }
                    elseif ($op -in 'Like', 'NotLike')
                    {
                        $join = ' OR '
                        if ($op -eq 'NotLike') { $join = ' AND ' }
                        $temp = foreach ($newValue in $value)
                        {
                            "[$col] $op2 @P$p"
                            $parameterDict["P$p"] = $newValue
                            $p += 1
                        }
                        $whereList.Add("($($temp -join $join))")
                    }
                    else
                    {
                        throw "Filter$op[$col] is an array, which is not compatible with '$op'."
                    }
                }
                else
                {
                    $parameterDict["P$p"] = $value
                    $whereList.Add("[$col] $op2 @P$p")
                }

                $p += 1
            }
        }

        foreach ($op in $otherDict.Keys)
        {
            $otherCol = $PSCmdlet.SessionState.PSVariable.GetValue("Filter$op")
            if (!$otherCol) { continue }
            $op2 = $otherDict.$op
            foreach ($col in $otherCol)
            {
                $whereList.Add("[$col] $op2")
            }
        }

        if ($whereList.Count)
        {
            " WHERE $($whereList -join ' AND ')"
        }
        else
        {
            ''
        }
        $parameterDict
    }
}

Function Get-DBRow
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Column,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $whereSql, $parameters = Get-DBWhereSql

        $query = "SELECT * FROM [$Schema].[$Table]$whereSql"

        Invoke-DBQuery $Connection $query -Mode Reader -Parameters $parameters
    }
}

Function Add-DBRow
{
    Param
    (
        [Parameter(ValueFromPipeline=$true)] [object] $InputObject,
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [switch] $BulkCopy
    )
    Begin
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $dataTable = New-Object System.Data.DataTable
        $tableAdapter = New-Object System.Data.SqlClient.SqlDataAdapter("SELECT * FROM [$Schema].[$Table]", $dbConnection.ConnectionObject)
        $commandBuilder = New-Object System.Data.SqlClient.SqlCommandBuilder $tableAdapter
        $tableAdapter.FillSchema($dataTable, [System.Data.SchemaType]::Mapped)

        $unexpected = @{}
    }
    Process
    {
        if (!$InputObject) { return }
        $newRow = $dataTable.NewRow()
        foreach ($property in $InputObject.PSObject.Properties)
        {
            $propertyName = $property.Name
            if ([System.DBNull]::Value.Equals($newRow[$propertyName]))
            {
                if ($null -ne $property.Value)
                {
                    $newRow[$propertyName] = $property.Value
                }
            }
            else
            {
                if (-not $unexpected.$propertyName)
                {
                    Write-Warning "InputObject has unexpected property '$propertyName.'"
                    $unexpected.$propertyName = $true
                }
            }
        }
        $dataTable.Rows.Add($newRow)
    }
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if ($PSCmdlet.ShouldProcess("$Schema.$Table", 'Insert Rows'))
        {
            if ($BulkCopy)
            {
                $options = [System.Data.SqlClient.SqlBulkCopyOptions]::FireTriggers +
                    [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock
                if (!$dbConnection.Transaction) { $options += [System.Data.SqlClient.SqlBulkCopyOptions]::UseInternalTransaction }
                $sqlBulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy $dbConnection.ConnectionObject, $options, $dbConnection.Transaction
                $sqlBulkCopy.DestinationTableName = "[$Schema].[$Table]"
                $sqlBulkCopy.WriteToServer($dataTable)
            }
            else
            {
                [void]$tableAdapter.Fill($dataTable)
                [void]$tableAdapter.Update($dataTable)
            }
        }
        $tableAdapter.Dispose()
    }
}

Function Remove-DBRow
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $whereSql, $parameters = Get-DBWhereSql

        $query = "DELETE FROM [$Schema].[$Table]$whereSql"

        $hasFilter = $parameters.Keys.Count -gt 0
        if ($hasFilter -or $PSCmdlet.ShouldProcess("$Schema.$Table", 'Remove All Rows'))
        {
            Invoke-DBQuery $Connection $query -Mode Scalar -Parameters $parameters
        }
    }
}

Function Set-DBRow
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [hashtable] $Set,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        if (!$Set.Keys.Count) { throw "One or more Set values are required." }

        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $whereSql, $parameters = Get-DBWhereSql
        $hasFilter = $parameters.Keys.Count -gt 0
        $sqlNameRegex = [regex]"\A[A-Za-z0-9 _\-]+\Z"

        $s = 0
        $setSqlList = foreach ($key in $Set.Keys)
        {
            if (!$sqlNameRegex.IsMatch($key)) { throw "Name '$($property.Name)' is not a valid SQL column name." }
            "[$key] = @S$s"
            $parameters["S$s"] = $Set.$key
            $s += 1
        }
        $setSql = $setSqlList -join ', '

        $query = "UPDATE [$Schema].[$Table] SET $setSql$whereSql"

        if ($hasFilter -or $PSCmdlet.ShouldProcess("$Schema.$Table", 'Update All Rows'))
        {
            Invoke-DBQuery $Connection $query -Mode Scalar -Parameters $parameters
        }
    }
}

Function Update-DBRow
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(ValueFromPipeline=$true)] [object] $InputObject,
        [Parameter()] [string[]] $Keys
    )
    Begin
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $finalKeys = $Keys
        if (!$finalKeys)
        {
            $finalKeys = Get-DBPrimaryKey -Connection $Connection -Schema $Schema -Table $Table -AsStringArray
        }
        if (!$finalKeys) { throw "$Schema.$Table has no primary keys. The Keys parameter must be provided." }
    }
    Process
    {
        if (!$InputObject) { return }
        $filterEq = @{}
        $set = @{}
        foreach ($property in $InputObject.PSObject.Properties)
        {
            if ($property.Name -in $finalKeys) { $filterEq[$property.Name] = $property.Value; continue }
            
            $set[$property.Name] = $property.Value
        }
        if ($filterEq.Keys.Count -ne @($finalKeys).Count)
        {
            Write-Error "InputObject '$InputObject' does not have all key properties."
            return
        }

        Set-DBRow -Connection $Connection -Schema $Schema -Table $Table -Set $set -FilterEq $filterEq
    }
}

Function Get-DBColumnSql
{
    Param
    (
        [Parameter(Mandatory=$true,Position=0)] [string] $Column,
        [Parameter(Mandatory=$true,Position=1)] [string] $Type,
        [Parameter()] [int] $Length,
        [Parameter()] [switch] $Required
    )
    End
    {
        $columnSql = "[$Column] $Type"
        if ($Type -match "char" -and -not $Length)
        {
            $columnSql += "(MAX)"
        }
        elseif ($Length)
        {
            $columnSql += "($Length)"
        }
        if ($Required) { $columnSql += " NOT NULL" }
        else { $columnSql += " NULL" }
        $columnSql
    }
}

Function Get-DBColumn
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter()] [string] $Schema,
        [Parameter()] [string] $Table,
        [Parameter()] [string[]] $Column
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        
        $filterEq = @{}
        if ($Table)
        {
            $filterEq['TABLE_NAME'] = $Table
        }
        if ($PSBoundParameters['Schema'] -or $Table)
        {
            $filterEq['TABLE_SCHEMA'] = $Schema
        }
        if ($Column)
        {
            $filterEq['COLUMN_NAME'] = $Column
        }

        Get-DBRow $Connection -Schema INFORMATION_SCHEMA -Table COLUMNS -FilterEq $filterEq
    }
}

Function New-DBColumn
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Column,
        [Parameter(Mandatory=$true)] [ValidateSet('nvarchar', 'nchar', 'varchar', 'char',
            'bigint', 'int', 'smallint', 'tinyint', 'bit',
            'numeric', 'decimal', 'money', 'smallmoney',
            'datetime', 'date', 'time',
            'ntext', 'varbinary', 'uniqueidentifier')] [string] $Type,
        [Parameter()] [int] $Length,
        [Parameter()] [switch] $Required
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $columnSql = Get-DBColumnSql $Column $Type -Length $Length -Required:$Required

        Invoke-DBQuery $Connection "ALTER TABLE [$Schema].[$Table] ADD $columnSql"
    }
}

Function Remove-DBColumn
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High', PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Column
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        if ($PSCmdlet.ShouldProcess("$Schema.$Table.$Column", 'Drop Column'))
        {
            Invoke-DBQuery $Connection "ALTER TABLE [$Schema].[$Table] DROP COLUMN [$Column]"
        }
    }
}

Function Rename-DBColumn
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Column,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $NewName
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        Invoke-DBQuery $Connection "EXEC sp_rename '[$Schema].[$Table].[$Column]', '$NewName', 'COLUMN';"
    }
}

Function Update-DBColumn
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Column,
        [Parameter(Mandatory=$true)] [ValidateSet('nvarchar', 'nchar', 'varchar', 'char',
            'bigint', 'int', 'smallint', 'tinyint', 'bit',
            'numeric', 'decimal', 'money', 'smallmoney',
            'datetime', 'date', 'time',
            'ntext', 'varbinary', 'uniqueidentifier')] [string] $Type,
        [Parameter()] [int] $Length,
        [Parameter()] [switch] $Required
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $columnSql = Get-DBColumnSql $Column $Type -Length $Length -Required:$Required
        if ($PSCmdlet.ShouldProcess("$Schema.$Table.$Column", 'Alter Column'))
        {
            Invoke-DBQuery $Connection "ALTER TABLE [$Schema].[$Table] ALTER COLUMN $columnSql"
        }
    }
}

Function Define-DBColumn
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Name,
        [Parameter(Mandatory=$true, Position=1)] [ValidateSet('nvarchar', 'nchar', 'varchar', 'char',
            'bigint', 'int', 'smallint', 'tinyint', 'bit',
            'numeric', 'decimal', 'money', 'smallmoney',
            'datetime', 'date', 'time',
            'ntext', 'varbinary', 'uniqueidentifier')] [string] $Type,
        [Parameter(Position=2)] [int] $Length,
        [Parameter()] [switch] $Required,
        [Parameter()] [switch] $PrimaryKey,
        [Parameter()] [switch] $Index,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $IndexName,
        [Parameter()] [switch] $Unique,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $UniqueIndexName
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        if ($PrimaryKey -and ($Unique.IsPresent -or $Index.IsPresent)) { throw "PrimaryKey and Unique or Index cannot be specified together." }
        if ($Unique.IsPresent -and $Index.IsPresent) { throw "Index and Unique cannot be specified together." }
        $definition = [ordered]@{}
        $definition.DefinitionType = 'Column'
        $definition.Name = $Name
        $definition.Type = $Type
        $definition.Length = $Length
        $definition.Required = $Required.IsPresent
        $definition.PrimaryKey = $PrimaryKey.IsPresent
        $definition.Index = $Index.IsPresent
        $definition.IndexName = $IndexName
        $definition.Unique = $Unique.IsPresent
        $definition.UniqueIndexName = $UniqueIndexName
        [pscustomobject]$definition
    }
}

Function Get-DBIndex
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Column
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $filterSqlList = @()
        $parameters = @{}
        if ($Table)
        {
            $filterSqlList += "t.name = @TableName"
            $parameters['TableName'] = $Table
        }
        if ($PSBoundParameters['Schema'] -or $Table)
        {
            $filterSqlList += "s.name = @SchemaName"
            $parameters['SchemaName'] = $Schema
        }
        if ($PSBoundParameters['Column'])
        {
            $filterSqlList += "col.name = @Column"
            $parameters['Column'] = $Column
        }
        $filterSql = $filterSqlList -join ' AND '
        if ($filterSql) { $filterSql = "AND $filterSql" }

        Invoke-DBQuery $Connection -Parameters $parameters -Query "
            SELECT
                s.name SchemaName,
                t.name TableName,
                ind.name IndexName,
                ind.index_id IndexId,
                ic.index_column_id ColumnId,
                col.name ColumnName,
                ind.is_primary_key IsPrimaryKey,
                ind.is_unique IsUnique,
                ind.is_unique_constraint IsUniqueConstraint
            FROM
                sys.indexes ind
            INNER JOIN
                sys.index_columns ic ON ind.object_id = ic.object_id and ind.index_id = ic.index_id
            INNER JOIN
                sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
            INNER JOIN
                sys.tables t ON ind.object_id = t.object_id
            INNER JOIN
                sys.schemas s ON t.schema_id = s.schema_id
            WHERE
                t.is_ms_shipped = 0
                $filterSql
            ORDER BY
                s.name, t.name, ind.name, ind.index_id, ic.index_column_id
        "
    }
}

Function New-DBIndex
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Column,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Index,
        [Parameter()] [ValidateSet('NonClustered', 'Clustered', 'Unique')] [string] $Type = 'NonClustered'
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if (!$Index)
        {
            if ($Column.Count -ne 1) { throw "The Index parameter must be specified if more than one Column is used." }
            if ($Type -eq 'Unique') { $Index = "AK_$($Column[0])" }
            else  { $Index = "IX_$($Column[0])" }
        }

        $columnNameSql = $(foreach ($c in $Column) { "[$c]" }) -join ','

        Invoke-DBQuery $Connection "CREATE $Type INDEX [$Index] ON [$Schema].[$Table] ($columnNameSql)"
    }
}

Function Remove-DBIndex
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High', PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Index
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $columnNameSql = $(foreach ($c in $Column) { "[$c]" }) -join ','

        if ($PSCmdlet.ShouldProcess("$Schema.$Table.$Index", 'Drop Index'))
        {
            Invoke-DBQuery $Connection "DROP INDEX [$Index] ON [$Schema].[$Table]"
        }
    }
}

Function Remove-DBConstraint
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High', PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Constraint
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if ($PSCmdlet.ShouldProcess("$Schema.$Table.$Constraint", 'Drop Constraint'))
        {
            Invoke-DBQuery $Connection "ALTER TABLE [$Schema].[$Table] DROP CONSTRAINT [$Constraint]"
        }
    }
}

Function Get-DBPrimaryKey
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [switch] $AsStringArray
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        if ($AsStringArray -and -not $Table) { throw "'AsStringArray' can only be provided if 'Table' is also provided." }

        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if ($AsStringArray)
        {
            $dataTable = New-Object System.Data.DataTable
            $tableAdapter = New-Object System.Data.SqlClient.SqlDataAdapter("SELECT * FROM [$Schema].[$Table]", $dbConnection.ConnectionObject)
            $commandBuilder = New-Object System.Data.SqlClient.SqlCommandBuilder $tableAdapter
            $tableAdapter.FillSchema($dataTable, [System.Data.SchemaType]::Mapped)
            $dataTable.Constraints |
                Where-Object IsPrimaryKey |
                ForEach-Object Columns |
                ForEach-Object ColumnName
            return
        }
        
        $filterSqlList = @()
        $parameters = @{}
        if ($Table)
        {
            $filterSqlList += "tab.[name] = @TableName"
            $parameters['TableName'] = $Table
        }
        if ($PSBoundParameters['Schema'] -or $Table)
        {
            $filterSqlList += "schema_name(tab.schema_id) = @SchemaName"
            $parameters['SchemaName'] = $Schema
        }
        $filterSql = $filterSqlList -join ' AND '
        if ($filterSql) { $filterSql = "WHERE $filterSql" }

        Invoke-DBQuery $Connection -Parameters $parameters -Query "
            SELECT schema_name(tab.schema_id) as [Schema],
                tab.[name] as [Table],
                pk.[name] as PrimaryKeyName,
                ic.index_column_id as ColumnId,
                col.[name] as ColumnName
            FROM sys.tables tab
                INNER JOIN sys.indexes pk
                    on tab.object_id = pk.object_id 
                    and pk.is_primary_key = 1
                INNER JOIN sys.index_columns ic
                    on ic.object_id = pk.object_id
                    and ic.index_id = pk.index_id
                INNER JOIN sys.columns col
                    on pk.object_id = col.object_id
                    and col.column_id = ic.column_id
            $filterSql
            ORDER BY schema_name(tab.schema_id),
                pk.[name],
                ic.index_column_id
        "
    }
}

Function New-DBPrimaryKey
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Column,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Name
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $primaryKeyName = $Name
        if (!$primaryKeyName) { $primaryKeyName = "PK_${Table}" }
        $columnNameSql = $(foreach ($c in $Column) { "[$c]" }) -join ','

        Invoke-DBQuery $Connection "ALTER TABLE [$Schema].[$Table] ADD CONSTRAINT [$primaryKeyName] PRIMARY KEY ($columnNameSql)"
    }
}

Function Define-DBPrimaryKey
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Column,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Name
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $definition = [ordered]@{}
        $definition.DefinitionType = 'PrimaryKey'
        $definition.Column = $Column
        $definition.Name = $Name
        [pscustomobject]$definition
    }
}
