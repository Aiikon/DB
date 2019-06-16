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

        $connection = 1 | Select-Object Name, Type, DefaultSchema, ConnectionObject
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

            $propertyList = $reader.GetSchemaTable() |
                Select-Object ColumnName, DataTypeName

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
        $dbConnection.ConnectionObject.GetSchema('Tables')
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
        [Parameter()] [hashtable] $FilterLike
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $query = "SELECT * FROM [$Schema].[$Table]"
        $parameters = @{}

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
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
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
            [void]$tableAdapter.Fill($dataTable)
            [void]$tableAdapter.Update($dataTable)
        }
        $tableAdapter.Dispose()
    }
}

Function Remove-DBRow
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [object] $Connection,
        [Parameter(Mandatory=$true)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Table,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string] $Schema
    )
    End
    {
        $dbConnection, $Schema = Connect-DBConnection $Connection $Schema
        $query = "SELECT * FROM [$Schema].[$Table]"
        $parameters = @{}

        $query = "DELETE FROM [$Schema].[$Table]"

        Invoke-DBQuery $Connection $query -Mode NonQuery -Parameters $parameters
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
            $columnSql = "    [$($columnDefinition.Name)] $($columnDefinition.Type)"
            if ($columnDefinition.Type -match "char" -and -not $columnDefinition.Length)
            {
                $columnSql += "(MAX)"
            }
            elseif ($columnDefinition.Length)
            {
                $columnSql += "($($columnDefinition.Length))"
            }
            if ($columnDefinition.Required) { $columnSql += " NOT NULL" }
            else { $columnSql += " NULL" }
            $definitionSqlList.Add($columnSql)
        }

        if ($primaryKeyDefinition)
        {
            $primaryKeyName = $primaryKeyDefinition.Name
            if (!$primaryKeyName) { $primaryKeyName = "${Table}_PrimaryKey" }
            $columnNameSql = $(foreach ($column in $primaryKeyDefinition.Column) { "[$column]" }) -join ','
            $definitionSqlList.Add("    CONSTRAINT [$primaryKeyName] PRIMARY KEY ($columnNameSql)")
        }

        $tableSql.Add($definitionSqlList -join ",`r`n")
        $tableSql.Add(")")

        Invoke-DBQuery $Connection ($tableSql -join "`r`n") -Mode NonQuery | Out-Null
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
        [Parameter()] [switch] $Unique
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
        $definition.Unique = $Unique.IsPresent
        [pscustomobject]$definition
    }
}

Function Define-DBPrimaryKey
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Column,
        [Parameter()] [ValidatePattern("\A[A-Za-z0-9 _\-]+\Z")] [string[]] $Name
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