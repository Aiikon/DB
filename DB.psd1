﻿#
# Module manifest for module 'DB'
#
# Generated by: Justin Coon
#
# Generated on: 3/20/2019
#

@{

# Script module or binary module file associated with this manifest.
# RootModule = ''

# Version number of this module.
ModuleVersion = '1.0'

# ID used to uniquely identify this module
GUID = 'ed9ef8e0-3067-4a34-b390-23c2c60d80b5'

# Author of this module
Author = 'Justin Coon'

# Company or vendor of this module
CompanyName = 'Unknown'

# Copyright statement for this module
Copyright = @'
(c) 2019 Justin Coon

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'@

# Description of the functionality provided by this module
# Description = ''

# Minimum version of the Windows PowerShell engine required by this module
# PowerShellVersion = ''

# Name of the Windows PowerShell host required by this module
# PowerShellHostName = ''

# Minimum version of the Windows PowerShell host required by this module
# PowerShellHostVersion = ''

# Minimum version of Microsoft .NET Framework required by this module
# DotNetFrameworkVersion = ''

# Minimum version of the common language runtime (CLR) required by this module
# CLRVersion = ''

# Processor architecture (None, X86, Amd64) required by this module
# ProcessorArchitecture = ''

# Modules that must be imported into the global environment prior to importing this module
# RequiredModules = @()

# Assemblies that must be loaded prior to importing this module
# RequiredAssemblies = @()

# Script files (.ps1) that are run in the caller's environment prior to importing this module.
# ScriptsToProcess = @()

# Type files (.ps1xml) to be loaded when importing this module
# TypesToProcess = @()

# Format files (.ps1xml) to be loaded when importing this module
# FormatsToProcess = @()

# Modules to import as nested modules of the module specified in RootModule/ModuleToProcess
NestedModules = @('DB.psm1')

# Functions to export from this module
FunctionsToExport = @(
    'Initialize-DBConnectionToLocalDB'
    'Use-DBTransaction'
    'Complete-DBTransaction'
    'Undo-DBTransaction'
    'Invoke-DBQuery'
    'Get-DBDatabase'
    'New-DBDatabase'
    'Remove-DBDatabase'
    'Get-DBSchema'
    'New-DBSchema'
    'Remove-DBSchema'
    'Get-DBTable'
    'New-DBTable'
    'Remove-DBTable'
    'Get-DBView'
    'New-DBView'
    'Remove-DBView'
    'Define-DBJoin'
    'Get-DBRow'
    'Add-DBRow'
    'Remove-DBRow'
    'Set-DBRow'
    'Update-DBRow'
    'Get-DBColumn'
    'New-DBColumn'
    'Remove-DBColumn'
    'Update-DBColumn'
    'Rename-DBColumn'
    'Define-DBColumn'
    'Get-DBIndex'
    'New-DBIndex'
    'Remove-DBIndex'
    'Remove-DBConstraint'
    'Get-DBPrimaryKey'
    'New-DBPrimaryKey'
    'Define-DBPrimaryKey'
    'New-DBTrigger'
    'Remove-DBTrigger'
    'New-DBAuditTable'
    'Remove-DBAuditTable'
)

# Cmdlets to export from this module
CmdletsToExport = @()

# Variables to export from this module
VariablesToExport = @()

# Aliases to export from this module
AliasesToExport = @()

# List of all modules packaged with this module
# ModuleList = @()

# List of all files packaged with this module
# FileList = @()

# Private data to pass to the module specified in RootModule/ModuleToProcess
# PrivateData = ''

# HelpInfo URI of this module
# HelpInfoURI = ''

# Default prefix for commands exported from this module. Override the default prefix using Import-Module -Prefix.
# DefaultCommandPrefix = ''

}

