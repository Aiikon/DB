if (!$Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections)
{
    $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5 = @{}
    $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5.Connections = @{}
}
$Script:ModuleConfig = $Global:ModuleConfig_ed9ef8e030674a34b39023c2c60d80b5

$Script:ColumnNameRegex = [regex]"\A[A-Za-z0-9 _\-]+\Z"

Function Initialize-DBConnectionToLocalDB
{
    Param
    (
        [Parameter(Mandatory=$true,Position=0)] [string] $ConnectionName,
        [Parameter(Mandatory=$true)] [string] $FilePath,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $DefaultSchema = 'dbo',
        [Parameter()] [int] $ConnectionTimeout
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

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
        if ($PSBoundParameters.ContainsKey('ConnectionTimeout')) { $connectionString = "${connectionString}Connection Timeout=$ConnectionTimeout;" }
        $connectionObject.ConnectionString = $connectionString
        $connectionObject.Open()

        $oldConnection = $Script:ModuleConfig.Connections[$ConnectionName]
        if ($oldConnection)
        {
            if ($oldConnection.ConnectionString -eq $connectionString)
            {
                if ($oldConnection.ConnectionObject.State -ne 'Open') { $oldConnection.ConnectionObject.Open() }
                return
            }
            try { $oldConnection.ConnectionObject.Close() } catch { } finally { $oldConnection.ConnectionObject.Dispose() }
        }

        $connection = [ordered]@{}
        $connection.Name = $ConnectionName
        $connection.Type = "LocalDB"
        $connection.DefaultSchema = $DefaultSchema
        $connection.ConnectionString = $connectionString
        $connection.ConnectionObject = $connectionObject
        $connection.Transaction = $null

        $Script:ModuleConfig.Connections[$ConnectionName] = [pscustomobject]$connection
    }
}

Function Initialize-DBConnectionToSqlDB
{
    Param
    (
        [Parameter(Mandatory=$true,Position=0)] [string] $ConnectionName,
        [Parameter(Mandatory=$true,Position=1)] [string] $Server,
        [Parameter(Position=2)] [string] $Instance,
        [Parameter(Position=3)] [string] $Database,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $DefaultSchema = 'dbo',
        [Parameter()] [int] $ConnectionTimeout
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        $inst = if ($Instance) { "\$Instance" }
        $datab = if ($Database) { ";Database=$Database" }
        $connectionString = "Server=$Server$inst$datab;Trusted_Connection=true;Integrated Security=true;"
        if ($PSBoundParameters.ContainsKey('ConnectionTimeout')) { $connectionString = "${connectionString}Connection Timeout=$ConnectionTimeout;" }
        $connectionObject = New-Object System.Data.SqlClient.SqlConnection
        $connectionObject.ConnectionString = $connectionString
        $connectionObject.Open()

        $oldConnection = $Script:ModuleConfig.Connections[$ConnectionName]
        if ($oldConnection)
        {
            if ($oldConnection.ConnectionString -eq $connectionString)
            {
                if ($oldConnection.ConnectionObject.State -ne 'Open') { $oldConnection.ConnectionObject.Open() }
                return
            }
            try { $oldConnection.ConnectionObject.Close() } catch { } finally { $oldConnection.ConnectionObject.Dispose() }
        }

        $connection = [ordered]@{}
        $connection.Name = $ConnectionName
        $connection.Type = "SqlDB"
        $connection.DefaultSchema = $DefaultSchema
        $connection.ConnectionString = $connectionString
        $connection.ConnectionObject = $connectionObject
        $connection.Transaction = $null

        $Script:ModuleConfig.Connections[$ConnectionName] = [pscustomobject]$connection
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

Function Close-DBConnection
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = $Script:ModuleConfig.Connections[$Connection]
        if (!$dbConnection) { throw "Connection '$Connection' is not defined." }
        $dbConnection.ConnectionObject.Close()
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
        [Parameter()] [Nullable[int]] $Timeout,
        [Parameter()] [string] [ValidateSet('Reader', 'NonQuery', 'Scalar')] $Mode = 'Reader'
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection = Connect-DBConnection $Connection

        $command = $dbConnection.ConnectionObject.CreateCommand()
        $command.CommandText = $Query
        if ($Timeout -ne $null) { $command.CommandTimeout = $Timeout }
        if ($dbConnection.Transaction) { $command.Transaction = $dbConnection.Transaction }
        $exception = $null

        "Final Query:", $Query | Write-Verbose

        foreach ($parameter in $Parameters.GetEnumerator())
        {
            [void]$command.Parameters.Add($parameter.Key, $parameter.Value)
        }

        $resultList = try
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

        $resultList

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
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [ValidateSet('Both', 'Table', 'View')] [string] $TableType = 'Both'
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
            $parameters.TableName = $Table
        }
        if ($PSBoundParameters['Schema'] -or $Table)
        {
            $filterSqlList += "s.name = @SchemaName"
            $parameters.SchemaName = $Schema
        }
        $filterSql = $filterSqlList -join ' AND '
        if ($filterSql) { $filterSql = "AND $filterSql" }

        $tableSelect = @(
            if ($TableType -in 'Both', 'Table')
            {
                "SELECT object_id, schema_id, name, is_ms_shipped, 'Table' TableType FROM sys.tables"
            }
            if ($TableType -in 'Both', 'View')
            {
                "SELECT object_id, schema_id, name, is_ms_shipped, 'View' TableType FROM sys.views"
            }
        ) -join ' UNION ALL '

        $tableList = Invoke-DBQuery $Connection -Parameters $parameters -ErrorAction Stop -Query "
            SELECT
                s.name [Schema],
                t.name [Table],
                t.TableType
            FROM (
                $tableSelect
            ) t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.is_ms_shipped = 0 $filterSql
            ORDER BY s.name, t.name
        "

        $tableList
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
        [Parameter(Mandatory=$true)] [scriptblock] $Definition,
        [Parameter()] [switch] $DebugOnly
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

        $temporalSettings = $definitionList | Where-Object DefinitionType -eq TemporalTableSettings
        if (@($temporalSettings).Count -gt 1) { throw "Only one Define-DBTemporalTableSettings can be provided." }

        $tableSql = New-Object System.Collections.Generic.List[string]
        $tableSql.Add("CREATE TABLE [$Schema].[$Table]")
        $tableSql.Add("(")

        $definitionSqlList = New-Object System.Collections.Generic.List[string]

        foreach ($columnDefinition in $columnDefinitionList)
        {
            $columnName = $columnDefinition.Name
            $columnSql = "    " + (Get-DBColumnSql $columnDefinition.Name $columnDefinition.Type -Length $columnDefinition.Length -Required:$columnDefinition.Required -Default $columnDefinition.Default -HasDefault $columnDefinition.HasDefault -IsIdentity $columnDefinition.IsIdentity)
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

        if ($temporalSettings)
        {
            $start, $end = $temporalSettings.SysStartTimeColumn, $temporalSettings.SysEndTimeColumn
            $definitionSqlList.Add("    [$start] datetime2 GENERATED ALWAYS AS ROW START")
            $definitionSqlList.Add("    [$end] datetime2 GENERATED ALWAYS AS ROW END")
            $definitionSqlList.Add("    PERIOD FOR SYSTEM_TIME ([$start], [$end])")
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

        if ($temporalSettings)
        {
            $historySchema = $temporalSettings.HistorySchema
            if (!$historySchema) { $historySchema = $Schema }
            $tableSql.Add("WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [$historySchema].[$($temporalSettings.HistoryTable)]))")
        }

        if ($DebugOnly) { return [pscustomobject]@{Query=$tableSql -join "`r`n"; Parameters=@{}} }

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

Function Define-DBTemporalTableSettings
{
    Param
    (
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $SysStartTimeColumn,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $SysEndTimeColumn,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $HistorySchema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $HistoryTable,
        [Parameter(Mandatory=$true)] [switch] $BetaAcknowledgement
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $definition = [ordered]@{}
        $definition.DefinitionType = 'TemporalTableSettings'
        $definition.SysStartTimeColumn = $SysStartTimeColumn
        $definition.SysEndTimeColumn = $SysEndTimeColumn
        $definition.HistorySchema = $HistorySchema
        $definition.HistoryTable = $HistoryTable
        [pscustomobject]$definition
    }
}

Function Get-DBViewSql
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $View,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        
        $filterSqlList = @()
        $parameters = @{}
        if ($View)
        {
            $filterSqlList += "v.name = @ViewName"
            $parameters.ViewName = $View
        }
        if ($PSBoundParameters['Schema'] -or $View)
        {
            $filterSqlList += "s.name = @SchemaName"
            $parameters.SchemaName = $Schema
        }
        $filterSql = $filterSqlList -join ' AND '
        if ($filterSql) { $filterSql = "AND $filterSql" }

        $tableList = Invoke-DBQuery $Connection -Parameters $parameters -ErrorAction Stop -Query "
            SELECT
                s.name [Schema],
                v.name [View],
                sm.definition [SQL]
            FROM sys.views v
                INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
                INNER JOIN sys.sql_modules sm ON v.object_id = sm.object_id
            WHERE v.is_ms_shipped = 0 $filterSql
            ORDER BY s.name, v.name
        "

        $tableList
    }
}

Function New-DBView
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High')]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $View,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [string] $SQL,
        [Parameter()] [switch] $Force
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if ($Force -and (Get-DBTable $Connection -Schema $Schema -Table $View) -and $PSCmdlet.ShouldProcess("$Schema.$View", 'Drop View'))
        {
            Remove-DBView $Connection -Schema $Schema -View $View -Confirm:$false
        }

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

$Script:FilterList = @(
    'FilterEq', 'FilterNe', 'FilterGt', 'FilterGe', 'FilterLt', 'FilterLe'
    'FilterLike', 'FilterNotLike', 'FilterNull', 'FilterNotNull', 'FilterNullOrEmpty', 'FilterExists'
)
Function Get-DBWhereSql
{
    [CmdletBinding()]
    Param
    (
        [Parameter()] $TablePrefix,
        [Parameter()] $ParameterDict = @{},
        [Parameter()] $ExistingSql,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull,
        [Parameter()] [string[]] $FilterNullOrEmpty,
        [Parameter()] [ValidateNotNullOrEmpty()] [object[]] $FilterExists
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $T = ''
        if ($TablePrefix) { $T = "$TablePrefix." }
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
        $otherDict.NullOrEmpty = ""

        $whereList = New-Object System.Collections.Generic.List[string]
        
        $p = @($ParameterDict.GetEnumerator()).Count
        foreach ($op in $opDict.Keys)
        {
            $filterDict = $PSCmdlet.SessionState.PSVariable.GetValue("Filter$op")
            if (!$filterDict) { continue }
            $op2 = $opDict[$op]
            foreach ($pair in $filterDict.GetEnumerator())
            {
                $col = $pair.Key
                $value = $pair.Value
                if ($col -notmatch "\A[A-Za-z0-9 _\-\*]+\Z") { throw "Column $col is an invalid column name." }
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
                            $whereList.Add("$T[$col] IN ($($temp -join ','))")
                        }
                        else
                        {
                            $whereList.Add("$T[$col] NOT IN ($($temp -join ','))")
                        }
                    }
                    elseif ($op -in 'Like', 'NotLike')
                    {
                        $join = ' OR '
                        if ($op -eq 'NotLike') { $join = ' AND ' }
                        $temp = foreach ($newValue in $value)
                        {
                            "$T[$col] $op2 @P$p"
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
                    $whereList.Add("$T[$col] $op2 @P$p")
                    $p += 1
                }
            }
        }

        foreach ($op in $otherDict.Keys)
        {
            $otherCol = $PSCmdlet.SessionState.PSVariable.GetValue("Filter$op")
            if (!$otherCol) { continue }
            $op2 = $otherDict.$op
            foreach ($col in $otherCol)
            {
                if ($col -notmatch "\A[A-Za-z0-9 _\-\*]+\Z") { throw "Column $col is an invalid column name." }
                if ($op -eq 'NullOrEmpty')
                {
                    $whereList.Add("($T[$col] IS NULL OR $T[$col] = '')")
                }
                else
                {
                    $whereList.Add("$T[$col] $op2")
                }
            }
        }

        if ($FilterExists)
        {
            $propertyNames = $FilterExists[0].PSObject.Properties.Name
            $badNames = @($propertyNames) -notmatch "\A[A-Za-z0-9 _\-]+\Z" -join ', '
            if ($badNames) { throw "The properties '$badNames' are invalid SQL column names." }

            $string = New-Object System.Text.StringBuilder

            $temp1 = foreach ($object in $FilterExists)
            {
                $temp2 = foreach ($property in $propertyNames)
                {
                    "$T[$property] = @P$p"
                    $parameterDict["P$p"] = $object.$property
                    $p += 1
                }
                $temp2 -join ' AND '
            }

            $whereList.Add("($($temp1 -join ' OR '))")
        }

        if ($whereList.Count)
        {
            if ($ExistingSql)
            {
                "$ExistingSql AND $($whereList -join ' AND ')"
            }
            else
            {
                " WHERE $($whereList -join ' AND ')"
            }
        }
        elseif ($ExistingSql)
        {
            $ExistingSql
        }
        else
        {
            ''
        }
        $parameterDict
    }
}

Function Define-DBJoin
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $LeftSchema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $LeftTable,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $LeftKey,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $RightSchema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $RightTable,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $RightKey,
        [Parameter()] [ValidateSet('Left', 'Inner', 'Right', 'FullOuter')] [string] $Type = 'Left',
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-\*]+\Z")] [string[]] $Column,
        [Parameter()] [hashtable] $Rename,
        [Parameter()] [hashtable] $CastNullAsBit,
        [Parameter()] [hashtable] $CountColumnAs,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull,
        [Parameter()] [string[]] $FilterNullOrEmpty,
        [Parameter()] [ValidateNotNullOrEmpty()] [object[]] $FilterExists,
        [Parameter()] [hashtable] $JoinFilterEq,
        [Parameter()] [hashtable] $JoinFilterNe,
        [Parameter()] [hashtable] $JoinFilterGt,
        [Parameter()] [hashtable] $JoinFilterGe,
        [Parameter()] [hashtable] $JoinFilterLt,
        [Parameter()] [hashtable] $JoinFilterLe,
        [Parameter()] [hashtable] $JoinFilterLike,
        [Parameter()] [hashtable] $JoinFilterNotLike,
        [Parameter()] [string[]] $JoinFilterNull,
        [Parameter()] [string[]] $JoinFilterNotNull,
        [Parameter()] [string[]] $JoinFilterNullOrEmpty,
        [Parameter()] [ValidateNotNullOrEmpty()] [object[]] $JoinFilterExists
    )
    End
    {
        $definition = [ordered]@{}
        $definition.DefinitionType = 'Join'
        $definition.LeftSchema = $LeftSchema
        $definition.LeftTable = $LeftTable
        $definition.LeftKey = $LeftKey
        $definition.RightSchema = $RightSchema
        $definition.RightTable = $RightTable
        $definition.RightKey = $RightKey
        $definition.Type = $Type
        $definition.Column = $Column
        $definition.Rename = $Rename
        $definition.CastNullAsBit = $CastNullAsBit
        $definition.CountColumnAs = $CountColumnAs
        foreach ($filter in $Script:FilterList)
        {
            $definition.$filter = $PSBoundParameters[$filter]
            $definition."Join$filter" = $PSBoundParameters["Join$filter"]
        }
        [pscustomobject]$definition
    }
}

Function Get-DBRow
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-\*]+\Z")] [string[]] $Column,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-\*]+\Z")] [string[]] $OrderBy,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [switch] $Unique,
        [Parameter()] [switch] $Count,
        [Parameter()] [int] $Top,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-\*]+\Z")] [string[]] $Sum,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-\*]+\Z")] [string[]] $Min,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-\*]+\Z")] [string[]] $Max,
        [Parameter()] [hashtable] $Rename,
        [Parameter()] [scriptblock] $Joins,
        [Parameter()] [Nullable[int]] $Timeout,
        [Parameter()] [switch] $DebugOnly,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull,
        [Parameter()] [string[]] $FilterNullOrEmpty,
        [Parameter()] [ValidateNotNullOrEmpty()] [object[]] $FilterExists
    )
    End
    {
        # Don't put a trap {} here or 'Select-Object -First' will throw a pipeline has been stopped exception.
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        try
        {
            $filterSplat = @{}
            foreach ($filter in $Script:FilterList)
            {
                if ($PSBoundParameters[$filter]) { $filterSplat[$filter] = $PSBoundParameters[$filter] }
            }
            $whereSql, $parameters = Get-DBWhereSql -TablePrefix T1 @filterSplat
        }
        catch { $PSCmdlet.ThrowTerminatingError($_) }

        $groupColumnDict = [ordered]@{}
        $columnSql = '*'
        $joinSql = ''

        if ($Column -or $Joins)
        {
            foreach ($c in $Column)
            {
                $name = $c
                if ($Rename -and $Rename[$c])
                {
                    $name = $Rename[$c]
                    if ($name -notmatch "\A[A-Za-z0-9 _\-\*]+\Z") { throw "Invalid Rename value: $name" }
                }
                $groupColumnDict.Add("T1.[$c]", "[$name]")
            }
            if (!$Column) { $groupColumnDict.Add("T1.*", $null) }
        }

        if ($Joins)
        {
            $joinTableDict = @{"[$Schema].[$Table]"='T1'}
            $t = 2
            $joinSqlList = @()
            $joinDefList = & $Joins
            foreach ($joinDef in $joinDefList)
            {
                $leftSchema = $joinDef.LeftSchema
                $leftTable = $joinDef.LeftTable
                $leftKey = $joinDef.LeftKey
                $rightSchema = $joinDef.RightSchema
                $rightTable = $joinDef.RightTable
                $rightKey = $joinDef.RightKey
                $joinColumn = $joinDef.Column
                if (!$leftSchema) { $leftSchema = $Schema }
                if (!$rightSchema) { $rightSchema = $Schema }
                if (!$leftTable) { $leftTable = $Table }
                if (!$leftKey) { $leftKey = $rightKey }
                if (!$rightKey) { $rightKey = $leftKey }
                if (!$leftKey -and !$rightKey) { throw "LeftKey, RightKey or both must be specified in Define-DBJoin." }

                $leftTb = $joinTableDict["[$leftSchema].[$leftTable]"]
                if (!$leftTb) { throw "[$leftSchema].[$leftTable] isn't an available table for joining on the left." }
                $rightTb = "T$t"
                if (!$joinTableDict["[$rightSchema].[$rightTable]"]) # Don't overwrite previous T0 shorthand
                {
                    $joinTableDict["[$rightSchema].[$rightTable]"] = $rightTb
                }
                $type = $joinDef.Type.ToUpper().Replace('FULLOUTER', 'FULL OUTER')

                $onList = for ($i = 0; $i -lt $leftKey.Count; $i++)
                {
                    "$leftTb.[$(@($leftKey)[$i])] = $rightTb.[$(@($rightKey)[$i])]"
                }
                $joinSql = " $type JOIN [$rightSchema].[$rightTable] $rightTb ON $($onList -join ' AND ')"

                $filterSplat = @{}
                foreach ($filter in $Script:FilterList)
                {
                    if ($joinDef."Join$filter") { $filterSplat[$filter] = $joinDef."Join$filter" }
                }

                $joinWhereSql, $parameters = Get-DBWhereSql -TablePrefix $rightTb -ParameterDict $parameters @filterSplat
                if ($joinWhereSql)
                {
                    $joinSql = "$joinSql$($joinWhereSql -replace "^ WHERE", " AND")"
                }

                $joinSqlList += $joinSql

                foreach ($c in $joinColumn)
                {
                    if ($c -eq '*') { $groupColumnDict.Add("$rightTb.*", $null); continue }
                    $name = $c
                    if ($joinDef.Rename.$c) { $name = $joinDef.Rename.$c }
                    if ($name -notmatch "\A[A-Za-z0-9 _\-]+\Z") { throw "Invalid Rename value: $name" }
                    $groupColumnDict.Add("$rightTb.[$c]", "[$name]")
                }

                if ($joinDef.CastNullAsBit) { foreach ($pair in $joinDef.CastNullAsBit.GetEnumerator()) {
                    $col = $pair.Key
                    if (!$Script:ColumnNameRegex.IsMatch($col)) { throw "Invalid CastNullAsBit column: $col" }
                    $as = $pair.Value
                    if (!$Script:ColumnNameRegex.IsMatch($as)) { throw "Invalid CastNullAsBit label: $as" }
                    $groupColumnDict.Add("CAST(IIF($rightTb.[$col] IS NULL, 0, 1) AS bit) [$as]", $null)
                } }

                if ($joinDef.CountColumnAs) { foreach ($pair in $joinDef.CountColumnAs.GetEnumerator()) {
                    $col = $pair.Key
                    if (!$Script:ColumnNameRegex.IsMatch($col)) { throw "Invalid CountColumnAs column: $col" }
                    $as = $pair.Value
                    if (!$Script:ColumnNameRegex.IsMatch($as)) { throw "Invalid CountColumnAs label: $as" }
                    $groupColumnDict.Add("COUNT($rightTb.[$col]) [$as]", $null)
                } }

                $filterSplat = @{}
                foreach ($filter in $Script:FilterList)
                {
                    if ($joinDef.$filter) { $filterSplat[$filter] = $joinDef.$filter }
                }

                $whereSql, $parameters = Get-DBWhereSql -TablePrefix $rightTb -ParameterDict $parameters -ExistingSql $whereSql @filtersplat

                $t += 1
            }
            $joinSql = $joinSqlList -join ''
        }

        if ($Rename -and @($Rename.GetEnumerator()).Count -and !$Column)
        {
            trap { $PSCmdlet.ThrowTerminatingError($_) }
            throw "-Column must be specified if -Rename is specified."
        }

        if (($Count -or $Sum -or $Min -or $Max) -and (!$Unique -or !$Column))
        {
            trap { $PSCmdlet.ThrowTerminatingError($_) }
            throw "-Column and -Unique must be specified if -Count, -Sum, -Min or -Max are specified."
        }

        $groupSql = ''
        if ($Unique)
        {
            trap { $PSCmdlet.ThrowTerminatingError($_) }
            if (!$Column) { throw "-Column must be specified if -Unique is specified." }
            $groupByNames = $groupColumnDict.GetEnumerator() | Where-Object Value | ForEach-Object Key
            $groupSql = " GROUP BY $($groupByNames -join ', ')"
            if ($Count)
            {
                $groupColumnDict.Add("COUNT(*) [Count]", $null)
            }
            $mathList = foreach ($math in 'Sum', 'Min', 'Max')
            {
                if ($PSBoundParameters[$math])
                {
                    foreach ($c in $PSBoundParameters[$math]) { [pscustomobject]@{Math=$math;Column=$c } }
                }
            }
            foreach ($mathGroup in $mathList | Group-Object Column)
            {
                foreach ($item in $mathGroup.Group)
                {
                    if ($mathGroup.Count -eq 1) { $groupColumnDict.Add("$($item.Math)(T1.$($item.Column)) [$($item.Column)]", $null) }
                    else { $groupColumnDict.Add("$($item.Math)(T1.$($item.Column)) [$($item.Column)$($item.Math)]", $null) }
                }
            }
        }
        
        $topSql = ''
        if ($Top)
        {
            $topSql = "TOP $Top "
        }

        $orderSql = ''
        if ($OrderBy)
        {
            $orderSql = " ORDER BY $($(foreach ($c in $OrderBy) { "T1.[$c]" }) -join ',')"
        }

        if (@($groupColumnDict.GetEnumerator()).Count)
        {
            $columnSql = @(
                foreach ($pair in $groupColumnDict.GetEnumerator())
                {
                    if ($pair.Value) { "$($pair.Key) $($pair.Value)" } else { $pair.Key }
                }
            ) -join ', '
        }

        $query = "SELECT $topSql$columnSql FROM [$Schema].[$Table] T1$joinSql$whereSql$groupSql$orderSql"

        if ($DebugOnly) { return [pscustomobject]@{Query=$query; Parameters=$parameters} }

        Invoke-DBQuery $Connection $query -Mode Reader -Parameters $parameters -Timeout $Timeout
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
        [Parameter()] [switch] $BulkCopy,
        [Parameter()] [Nullable[int]] $Timeout
    )
    Begin
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $dataTable = New-Object System.Data.DataTable
        $selectCommand = $dbConnection.ConnectionObject.CreateCommand()
        $selectCommand.CommandText = "SELECT * FROM [$Schema].[$Table]"
        if ($dbConnection.Transaction) { $selectCommand.Transaction = $dbConnection.Transaction }
        $tableAdapter = New-Object System.Data.SqlClient.SqlDataAdapter $selectCommand
        $commandBuilder = New-Object System.Data.SqlClient.SqlCommandBuilder $tableAdapter
        $tableAdapter.FillSchema($dataTable, [System.Data.SchemaType]::Mapped)
        
        $whitespaceMustBeNull = @{}
        foreach ($column in $dataTable.Columns)
        {
            if ($column.DataType -eq [System.TimeSpan]) { $whitespaceMustBeNull[$column.ColumnName] = $true }
        }

        $unexpected = @{}
        $removedUnused = $false
    }
    Process
    {
        if (!$InputObject) { return }
        if (!$removedUnused)
        {
            $removedUnused = $true
            foreach ($column in @($dataTable.Columns))
            {
                if ($column.ColumnName -in $dataTable.PrimaryKey.ColumnName) { continue }
                if (!$InputObject.PSObject.Properties[$column.ColumnName]) { $dataTable.Columns.Remove($column) }
            }
        }
        $newRow = $dataTable.NewRow()
        foreach ($property in $InputObject.PSObject.Properties)
        {
            $propertyName = $property.Name
            if ([System.DBNull]::Value.Equals($newRow[$propertyName]))
            {
                if ($null -ne $property.Value -and -not ($whitespaceMustBeNull[$propertyName] -and $property.Value -eq ''))
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
                if ($Timeout -ne $null) { $sqlBulkCopy.BulkCopyTimeout = $Timeout }
                $sqlBulkCopy.WriteToServer($dataTable)
                $sqlBulkCopy.Close()
            }
            else
            {
                if ($Timeout -ne $null) { throw "Timeout is not yet implemented." }
                $colList = foreach ($column in $dataTable.Columns) { "[$($column.ColumnName)]" } # Rebuild from columns that remain in the datatable
                $tableAdapter.SelectCommand.CommandText = "SELECT $($colList -join ', ') FROM [$Schema].[$Table]"
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
        [Parameter()] [Nullable[int]] $Timeout,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull,
        [Parameter()] [string[]] $FilterNullOrEmpty,
        [Parameter()] [ValidateNotNullOrEmpty()] [object[]] $FilterExists
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $filterSplat = @{}
        foreach ($filter in $Script:FilterList)
        {
            if ($PSBoundParameters[$filter]) { $filterSplat[$filter] = $PSBoundParameters[$filter] }
        }

        $whereSql, $parameters = Get-DBWhereSql @filterSplat

        $query = "DELETE FROM [$Schema].[$Table]$whereSql"

        $hasFilter = @($parameters.GetEnumerator()).Count -gt 0
        if ($hasFilter -or $PSCmdlet.ShouldProcess("$Schema.$Table", 'Remove All Rows'))
        {
            Invoke-DBQuery $Connection $query -Mode Scalar -Parameters $parameters -Timeout $Timeout
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
        [Parameter()] [Nullable[int]] $Timeout,
        [Parameter()] [hashtable] $FilterEq,
        [Parameter()] [hashtable] $FilterNe,
        [Parameter()] [hashtable] $FilterGt,
        [Parameter()] [hashtable] $FilterGe,
        [Parameter()] [hashtable] $FilterLt,
        [Parameter()] [hashtable] $FilterLe,
        [Parameter()] [hashtable] $FilterLike,
        [Parameter()] [hashtable] $FilterNotLike,
        [Parameter()] [string[]] $FilterNull,
        [Parameter()] [string[]] $FilterNotNull,
        [Parameter()] [string[]] $FilterNullOrEmpty,
        [Parameter()] [ValidateNotNullOrEmpty()] [object[]] $FilterExists
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }

        if (!@($Set.GetEnumerator()).Count) { throw "One or more Set values are required." }

        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        $filterSplat = @{}
        foreach ($filter in $Script:FilterList)
        {
            if ($PSBoundParameters[$filter]) { $filterSplat[$filter] = $PSBoundParameters[$filter] }
        }

        $whereSql, $parameters = Get-DBWhereSql @filterSplat
        $hasFilter = @($parameters.GetEnumerator()).Count -gt 0
        $sqlNameRegex = [regex]"\A[A-Za-z0-9 _\-]+\Z"

        $s = 0
        $setSqlList = foreach ($key in @($Set.GetEnumerator()).Key)
        {
            if (!$sqlNameRegex.IsMatch($key)) { throw "Name '$($property.Name)' is not a valid SQL column name." }
            "[$key] = @S$s"
            $value = $Set[$key]
            if ($value -eq $null) { $value = [DBNull]::Value }
            $parameters["S$s"] = $value
            $s += 1
        }
        $setSql = $setSqlList -join ', '

        $query = "UPDATE [$Schema].[$Table] SET $setSql$whereSql"

        if ($hasFilter -or $PSCmdlet.ShouldProcess("$Schema.$Table", 'Update All Rows'))
        {
            Invoke-DBQuery $Connection $query -Mode Scalar -Parameters $parameters -Timeout $Timeout
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
        [Parameter()] [string[]] $Keys,
        [Parameter()] [Nullable[int]] $Timeout
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
        if (@($filterEq).GetEnumerator().Count -ne @($finalKeys).Count)
        {
            Write-Error "InputObject '$InputObject' does not have all key properties."
            return
        }

        Set-DBRow -Connection $Connection -Schema $Schema -Table $Table -Set $set -FilterEq $filterEq -Timeout $Timeout
    }
}

Function Get-DBColumnSql
{
    Param
    (
        [Parameter(Mandatory=$true,Position=0)] [string] $Column,
        [Parameter(Mandatory=$true,Position=1)] [string] $Type,
        [Parameter()] [int] $Length,
        [Parameter()] [switch] $Required,
        [Parameter()] [string] $Default,
        [Parameter()] [bool] $HasDefault,
        [Parameter()] [bool] $IsIdentity
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
        if ($HasDefault)
        {
            if ($Type -match 'char|time') { $Default = "'$Default'" }
            $columnSql += " DEFAULT $Default"
        }
        if ($IsIdentity)
        {
            $columnSql += " IDENTITY(1,1)"
        }
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
        [Parameter()] [string] $Column
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
            $parameters.TableName = $Table
        }
        if ($PSBoundParameters['Schema'] -or $Table)
        {
            $filterSqlList += "s.name = @SchemaName"
            $parameters.SchemaName = $Schema
        }
        if ($Column)
        {
            $filterSqlList += "col.name = @ColumnName"
            $parameters.ColumnName = $Column
        }
        $filterSql = $filterSqlList -join ' AND '
        if ($filterSql) { $filterSql = "AND $filterSql" }

        Invoke-DBQuery $Connection -Parameters $parameters -Query "
            SELECT
                s.name [Schema],
                t.name [Table],
                c.name [Column],
                ty.name Type,
                IIF(c.max_length = -1 OR ty.name NOT IN ('char', 'varchar', 'nchar', 'nvarchar', 'varbinary', 'xml'),
                    NULL, c.max_length) Length,
                c.column_id Position,
                c.is_nullable IsNullable,
                ISNULL(ici.is_primary_key, 0) IsPrimaryKey,
                c.is_identity IsIdentity
            FROM sys.columns c
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            LEFT OUTER JOIN (
                SELECT ic.object_id, ic.column_id, i.is_primary_key
                FROM sys.index_columns ic
                    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        AND i.is_primary_key = 1
            ) ici ON ici.object_id = c.object_id AND ici.column_id = c.column_id
            INNER JOIN (
                SELECT object_id, schema_id, name, is_ms_shipped FROM sys.tables
                UNION ALL
                SELECT object_id, schema_id, name, is_ms_shipped FROM sys.views
            ) t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.is_ms_shipped = 0 $filterSql
            ORDER BY s.name, t.name, c.column_id
        "
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
            'numeric', 'decimal', 'float', 'money', 'smallmoney',
            'datetime', 'date', 'time',
            'ntext', 'varbinary', 'uniqueidentifier', 'xml')] [string] $Type,
        [Parameter()] [int] $Length,
        [Parameter()] [switch] $Required,
        [Parameter()] [string] $Default,
        [Parameter()] [switch] $DebugOnly
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $columnSql = Get-DBColumnSql $Column $Type -Length $Length -Required:$Required -Default $Default -HasDefault $PSBoundParameters.ContainsKey('Default')
        $finalSql = "ALTER TABLE [$Schema].[$Table] ADD $columnSql"

        if ($DebugOnly) { return [pscustomobject]@{Query=$finalSql; Parameters=@{}} }

        Invoke-DBQuery $Connection $finalSql
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
            'numeric', 'decimal', 'float', 'money', 'smallmoney',
            'datetime', 'date', 'time',
            'ntext', 'varbinary', 'uniqueidentifier', 'xml')] [string] $Type,
        [Parameter()] [int] $Length,
        [Parameter()] [switch] $Required,
        [Parameter()] [string] $Default,
        [Parameter()] [switch] $DebugOnly
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $columnSql = Get-DBColumnSql $Column $Type -Length $Length -Required:$Required

        $query = [System.Text.StringBuilder]::new()

        [void]$query.Append("BEGIN ALTER TABLE [$Schema].[$Table] ALTER COLUMN $columnSql;")

        if ($PSBoundParameters.ContainsKey('Default'))
        {
            if ($Type -match "char|time") { $Default = "'$Default'" }
            [void]$query.Append(" ALTER TABLE [$Schema].[$Table] ADD CONSTRAINT [DF_${Schema}_${Table}_${Column}] DEFAULT $Default FOR [$Column];")
        }
        [void]$query.Append(' END;')

        if ($PSCmdlet.ShouldProcess("$Schema.$Table.$Column", 'Alter Column'))
        {
            if ($DebugOnly) { [pscustomobject]@{Query=$query.ToString(); Parameters=@{}} }
            else { Invoke-DBQuery $Connection $query.ToString() }
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
            'numeric', 'decimal', 'float', 'money', 'smallmoney',
            'datetime', 'date', 'time',
            'ntext', 'varbinary', 'uniqueidentifier', 'xml')] [string] $Type,
        [Parameter(Position=2)] [int] $Length,
        [Parameter()] [switch] $Required,
        [Parameter()] [switch] $PrimaryKey,
        [Parameter()] [string] $Default,
        [Parameter()] [switch] $Index,
        [Parameter()] [switch] $Identity,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $IndexName,
        [Parameter()] [switch] $Unique,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $UniqueIndexName
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        if ($PrimaryKey -and ($Unique.IsPresent -or $Index.IsPresent)) { throw "PrimaryKey and Unique or Index cannot be specified together." }
        if ($Unique.IsPresent -and $Index.IsPresent) { throw "Index and Unique cannot be specified together." }
        if ($Type -match "^(n?)char$" -and !$Length)
        {
            $newType = "$($Matches[1])varchar"
            Write-Warning "Changing $Type to $newType because it has no length."
            $Type = $newType
        }
        $definition = [ordered]@{}
        $definition.DefinitionType = 'Column'
        $definition.Name = $Name
        $definition.Type = $Type
        $definition.Length = $Length
        $definition.Required = $Required.IsPresent
        $definition.PrimaryKey = $PrimaryKey.IsPresent
        $definition.HasDefault = $PSBoundParameters.ContainsKey('Default')
        $definition.Default = $Default
        $definition.Index = $Index.IsPresent
        $definition.IndexName = $IndexName
        $definition.IsIdentity = $Identity.IsPresent
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

Function Get-DBTrigger
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Trigger,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        trap { $PSCmdlet.ThrowTerminatingError($_) }
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        
        $filterSqlList = @()
        $parameters = @{}
        if ($Trigger)
        {
            $filterSqlList += "tr.name = @Trigger"
            $parameters.Trigger = $Trigger
        }
        if ($PSBoundParameters['Schema'] -or $Table)
        {
            $filterSqlList += "t.name = @Table"
            $parameters.Table = $Table
        }
        if ($PSBoundParameters['Schema'] -or $Trigger -or $Table)
        {
            $filterSqlList += "s.name = @Schema"
            $parameters.Schema = $Schema
        }
        $filterSql = $filterSqlList -join ' AND '
        if ($filterSql) { $filterSql = "AND $filterSql" }

        $triggerList = Invoke-DBQuery $Connection -Parameters $parameters -ErrorAction Stop -Query "
            SELECT
                s.name [Schema],
                t.name [Table],
                tr.name [Trigger],
                sm.definition [SQL]
            FROM sys.triggers tr
                INNER JOIN sys.tables t ON tr.parent_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.sql_modules sm ON tr.object_id = sm.object_id
            WHERE tr.is_ms_shipped = 0 $filterSql
            ORDER BY s.name, t.name, tr.name
        "

        $triggerList
    }
}

Function New-DBTrigger
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidateSet('Insert', 'Update', 'Delete')] [string] $TriggerFor,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Trigger,
        [Parameter(Mandatory=$true)] [string] $SQL
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if (-not $Trigger) { $Trigger = "TR_$Table`_$TriggerFor" }

        Invoke-DBQuery $Connection -Mode NonQuery -Query "
            CREATE TRIGGER [$Schema].[$Trigger] ON [$Schema].[$Table] FOR $TriggerFor
            AS BEGIN
            $SQL
            END
        " | Out-Null
    }
}

Function Remove-DBTrigger
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High', PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [ValidateSet('Insert', 'Update', 'Delete')] [string] $TriggerFor,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Trigger
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if (-not $Trigger) { $Trigger = "TR_$Table`_$TriggerFor" }

        if ($PSCmdlet.ShouldProcess("$Schema.$Trigger", 'Drop Trigger'))
        {
            Invoke-DBQuery $Connection "DROP TRIGGER [$Schema].[$Trigger]"
        }
    }
}

Function New-DBForeignKeyConstraint
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Column,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $ForeignTable,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $ForeignSchema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $ForeignColumn,
        [Parameter()] [ValidateSet('NoAction', 'Cascade')] [string] $OnUpdate = 'Cascade',
        [Parameter()] [ValidateSet('NoAction', 'Cascade')] [string] $OnDelete,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Constraint,
        [Parameter()] [switch] $DebugOnly
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        if (!$ForeignSchema) { $ForeignSchema = $Schema }
        if (!$ForeignColumn) { $ForeignColumn = $Column }
        if (!$Constraint) { $Constraint = "FK_${Table}_${Column}" }
        $query = [System.Text.StringBuilder]::new()
        [void]$query.Append("ALTER TABLE [$Schema].[$Table]")
        [void]$query.Append(" ADD CONSTRAINT [$Constraint] FOREIGN KEY ([$Column])")
        [void]$query.Append(" REFERENCES [$ForeignSchema].[$ForeignTable] ([$ForeignColumn])")
        if ($OnUpdate) { [void]$query.Append(" ON UPDATE $($OnUpdate.ToUpper() -replace 'NOACTION', 'NO ACTION')") }
        if ($OnDelete) { [void]$query.Append(" ON UPDATE $($OnDelete.ToUpper() -replace 'NOACTION', 'NO ACTION')") }
        
        if ($PSCmdlet.ShouldProcess("$Schema.$Table.$Constraint", 'Create Foreign Key Constraint'))
        {
            if ($DebugOnly) { return [pscustomobject]@{Query=$query.ToString(); Parameters=@{}} }
            Invoke-DBQuery $Connection -Mode NonQuery -Query $query.ToString()
        }
    }
}

Function New-DBAuditTable
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $AuditSchema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $AuditTable,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $AuditBefore,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [switch] $IncludeAfter,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $TriggerPrefix
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if (!$AuditTable) { $AuditTable = "${Table}_Audit" }
        if (!$AuditSchema) { $AuditSchema = $Schema }
        if (!$TriggerPrefix) { $TriggerPrefix = "TR_${Schema}_${Table}_Audit" }

        $primaryKeyList = Get-DBPrimaryKey $Connection -Schema $Schema -Table $Table -AsStringArray
        $columnList = Get-DBColumn $Connection -Schema $Schema -Table $Table

        if (!$AuditBefore) { $AuditBefore = @($columnList.Column) }
        $AuditBefore = @($(foreach ($c in $AuditBefore) { if ($c -notin $primaryKeyList) { $c } }))
        $AuditAfter = @()
        if ($IncludeAfter) { $AuditAfter = $AuditBefore } # Simplifies foreach loops

        New-DBTable $Connection -Schema $AuditSchema -Table $AuditTable -WarningAction SilentlyContinue -Definition {
            foreach ($column in $columnList)
            {
                $name = $column.Column
                if ($column.Column -in $primaryKeyList) { Define-DBColumn $name $column.Type -Length $column.Length -Required }
                elseif ($column.Column -in $AuditBefore)
                {
                    Define-DBColumn "${name}__Updated" bit
                    Define-DBColumn "${name}__Before" $column.Type
                    if ($IncludeAfter) { Define-DBColumn "${name}__After" $column.Type }
                }
            }

            Define-DBColumn __Timestamp datetime -Required
            Define-DBColumn __Username nvarchar -Required
            Define-DBColumn __Type char 1 -Required
        }

        New-DBIndex $Connection -Schema $AuditSchema -Table $AuditTable -Column $primaryKeyList -Index IX_PrimaryKey

        New-DBTrigger $Connection -Schema $Schema -Table $Table -TriggerFor Insert -Trigger "${TriggerPrefix}_Insert" -SQL "
            IF @@ROWCOUNT = 0 RETURN
            SET NOCOUNT ON

            INSERT INTO [$AuditSchema].[$AuditTable]
            (
                $($(foreach ($k in $primaryKeyList) { "[$k]," }) -join '')
                $($(foreach ($c in $AuditAfter) { "[${c}__After]," }) -join '')
                __Timestamp,
                __Username,
                __Type
            )
            SELECT
                $($(foreach ($k in $primaryKeyList) { "I.[$k]," }) -join '')
                $($(foreach ($c in $AuditAfter) { "I.[${c}]," }) -join '')
                getutcdate(),
                suser_sname(),
                'I'
            FROM INSERTED I
            IF @@ERROR <> 0 BEGIN
                raiserror ('Could not record insert audit in [$AuditSchema].[$AuditTable]; operation will be cancelled.', 16, 1)
                ROLLBACK TRANSACTION
            END
        "

        New-DBTrigger $Connection -Schema $Schema -Table $Table -TriggerFor Update -Trigger "${TriggerPrefix}_Update" -SQL "
            IF @@ROWCOUNT = 0 RETURN
            SET NOCOUNT ON

            INSERT INTO [$AuditSchema].[$AuditTable]
            (
                $($(foreach ($k in $primaryKeyList) { "[$k]," }) -join '')
                $($(foreach ($c in $AuditBefore) { "[${c}__Updated]," }) -join '')
                $($(foreach ($c in $AuditBefore) { "[${c}__Before]," }) -join '')
                $($(foreach ($c in $AuditAfter) { "[${c}__After]," }) -join '')
                __Timestamp,
                __Username,
                __Type
            )
            SELECT
                $($(foreach ($k in $primaryKeyList) { "I.[$k]," }) -join '')
                $($(foreach ($c in $AuditBefore) { "CASE WHEN (isnull(D.[$c], '') <> isnull(I.[$c], '')) THEN 1 ELSE 0 END," }) -join '')
                $($(foreach ($c in $AuditBefore) { "D.[$c]," }) -join '')
                $($(foreach ($c in $AuditAfter) { "I.[$c]," }) -join '')
                getutcdate(),
                suser_sname(),
                'U'
            FROM DELETED D INNER JOIN INSERTED I ON $($(foreach ($k in $primaryKeyList) { "D.[$k] = I.[$k]" }) -join ' AND ')
            $(if($AuditBefore) { 'WHERE' })
                $($(foreach ($c in $AuditBefore) { "isnull(D.[$c], '') <> isnull(I.[$c], '')" }) -join ' OR ')
            IF @@ERROR <> 0 BEGIN
                raiserror ('Could not record update audit in [$AuditSchema].[$AuditTable]; operation will be cancelled.', 16, 1)
                ROLLBACK TRANSACTION
            END
        "

        New-DBTrigger $Connection -Schema $Schema -Table $Table -TriggerFor Delete -Trigger "${TriggerPrefix}_Delete" -SQL "
            IF @@ROWCOUNT = 0 RETURN
            SET NOCOUNT ON

            INSERT INTO [$AuditSchema].[$AuditTable]
            (
                $($(foreach ($k in $primaryKeyList) { "[$k]," }) -join '')
                $($(foreach ($c in $AuditBefore) { "[${c}__Before]," }) -join '')
                __Timestamp,
                __Username,
                __Type
            )
            SELECT
                $($(foreach ($k in $primaryKeyList) { "D.[$k]," }) -join '')
                $($(foreach ($c in $AuditBefore) { "D.[$c]," }) -join '')
                getutcdate(),
                suser_sname(),
                'D'
            FROM DELETED D
            IF @@ERROR <> 0 BEGIN
                raiserror ('Could not record delete audit in [$AuditSchema].[$AuditTable]; operation will be cancelled.', 16, 1)
                ROLLBACK TRANSACTION
            END
        "
    }
}

Function Remove-DBAuditTable
{
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='High', PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $AuditSchema,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $AuditTable
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema

        if (!$AuditTable) { $AuditTable = "${Table}_Audit" }
        if (!$AuditSchema) { $AuditSchema = $Schema }
        if (!$TriggerPrefix) { $TriggerPrefix = "TR_${Schema}_${Table}_Audit" }

        if ($PSCmdlet.ShouldProcess("$AuditSchema.$AuditTable", 'Drop Audit Table and Triggers'))
        {
            Remove-DBTrigger $Connection -Schema $Schema -Table $Table -TriggerFor Insert -Trigger "${TriggerPrefix}_Insert" -Confirm:$false
            Remove-DBTrigger $Connection -Schema $Schema -Table $Table -TriggerFor Update -Trigger "${TriggerPrefix}_Update" -Confirm:$false
            Remove-DBTrigger $Connection -Schema $Schema -Table $Table -TriggerFor Delete -Trigger "${TriggerPrefix}_Delete" -Confirm:$false
            Remove-DBTable $Connection -Schema $AuditSchema -Table $AuditTable -Confirm:$false
        }
    }
}

Function Update-DBIntellisense
{
    [CmdletBinding(PositionalBinding=$false)]
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [string] $Connection
    )
    End
    {
        $Script:ColumnDict = @{}
        $Script:ColumnDict[$Connection] = Get-DBColumn $connectionName
    }
}

$Script:ColumnDict = @{}

foreach ($command in (Get-Command -Module DB))
{
    if (!$command.Parameters.ContainsKey('Connection')) { continue }

    Register-ArgumentCompleter -CommandName $command.Name -ParameterName 'Connection' -ScriptBlock {
        Param ($CommandName, $ParameterName, $WordToComplete, $CommandAst, $FakeBoundParameter)
        $wordRegex = [regex]::Escape($WordToComplete)
        foreach ($value in $Script:ModuleConfig.Connections.Keys | Sort-Object)
        {
            if ($value -match $wordRegex)
            {
                [System.Management.Automation.CompletionResult]::new($value)
            }
        }
    }

    if (!$command.Parameters.ContainsKey('Schema')) { continue }

    Register-ArgumentCompleter -CommandName $command.Name -ParameterName 'Schema' -ScriptBlock {
        Param ($CommandName, $ParameterName, $WordToComplete, $CommandAst, $FakeBoundParameter)
        $connectionName = $FakeBoundParameter['Connection']
        if (!$connectionName) { return }
        if (!$Script:ColumnDict.ContainsKey($connectionName))
        {
            $Script:ColumnDict[$connectionName] = Get-DBColumn $connectionName
        }
        $schemaList = $Script:ColumnDict[$connectionName] |
            Select-Object -Unique -ExpandProperty Schema |
            Sort-Object
        $wordRegex = [regex]::Escape($WordToComplete)
        foreach ($value in $schemaList)
        {
            if ($value -match $wordRegex)
            {
                [System.Management.Automation.CompletionResult]::new($value)
            }
        }
    }

    if (!$command.Parameters.ContainsKey('Table')) { continue }

    Register-ArgumentCompleter -CommandName $command.Name -ParameterName 'Table' -ScriptBlock {
        Param ($CommandName, $ParameterName, $WordToComplete, $CommandAst, $FakeBoundParameter)
        $connectionName = $FakeBoundParameter['Connection']
        if (!$connectionName) { return }
        if (!$Script:ColumnDict.ContainsKey($connectionName))
        {
            $Script:ColumnDict[$connectionName] = Get-DBColumn $connectionName
        }
        $schemaName = $FakeBoundParameter['Schema']
        if (!$schemaName) { $schemaName = $Script:ModuleConfig.Connections[$connectionName].DefaultSchema }

        $tableList = $Script:ColumnDict[$connectionName] |
            Where-Object Schema -eq $schemaName |
            Select-Object -Unique -ExpandProperty Table |
            Sort-Object

        $wordRegex = [regex]::Escape($WordToComplete)
        foreach ($value in $tableList)
        {
            if ($value -match $wordRegex)
            {
                [System.Management.Automation.CompletionResult]::new($value)
            }
        }
    }

    if (!$command.Parameters.ContainsKey('Column')) { continue }

    Register-ArgumentCompleter -CommandName $command.Name -ParameterName 'Column' -ScriptBlock {
        Param ($CommandName, $ParameterName, $WordToComplete, $CommandAst, $FakeBoundParameter)
        $connectionName = $FakeBoundParameter['Connection']
        if (!$connectionName) { return }
        if (!$Script:ColumnDict.ContainsKey($connectionName))
        {
            $Script:ColumnDict[$connectionName] = Get-DBColumn $connectionName
        }
        $schemaName = $FakeBoundParameter['Schema']
        if (!$schemaName) { $schemaName = $Script:ModuleConfig.Connections[$connectionName].DefaultSchema }

        $tableName = $FakeBoundParameter['Table']
        if (!$tableName) { return }

        $columnList = $Script:ColumnDict[$connectionName] |
            Where-Object Schema -eq $schemaName |
            Where-Object Table -eq $tableName |
            Select-Object -Unique -ExpandProperty Column |
            Sort-Object
            
        $wordRegex = [regex]::Escape($WordToComplete)
        foreach ($value in $columnList)
        {
            [System.Management.Automation.CompletionResult]::new($value)
        }
    }
}
