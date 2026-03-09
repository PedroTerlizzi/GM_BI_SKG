<!--#include virtual="/_include/report_head.asp"-->
<%
'------------------------------------------------------------------------------
' EXECUTIVE HEADER
'------------------------------------------------------------------------------
' This single ASP file operates as a governed BI execution engine over Flowww.
' It resolves authenticated identity, enforces permissioned scope, orchestrates
' read-only dataset extraction, executes the render pipeline, and records
' execution telemetry for data governance and runtime observability.
'
'------------------------------------------------------------------------------
' ARCHITECTURE
'------------------------------------------------------------------------------
' AUTH
' - responsibility: resolve identity and trusted session context.
' - inputs: Flowww Session values and request/server metadata.
' - outputs: Auth* context consumed by scope resolution and governance checks.
'
' SCOPE
' - responsibility: compute permission-filtered execution scope.
' - inputs: Auth* context, policies, requested filters/dates/clinics.
' - outputs: Effective* scope contract used by engine and SQL layers.
'
' ENGINE
' - responsibility: orchestrate runtime control and report execution.
' - inputs: Effective* scope, report configuration, runtime action.
' - outputs: execution state, datasets, render payloads, telemetry payloads.
'
' SQL
' - responsibility: execute governed read-only dataset extraction.
' - inputs: SQL templates, engine scope parameters, safety validators.
' - outputs: tabular datasets for filters, KPIs, graph, and main report.
'
' RENDER
' - responsibility: transform datasets into render-ready payloads.
' - inputs: datasets, metadata, configuration flags, runtime context.
' - outputs: HTML fragments, JSON payloads, export streams.
'
' LOGS
' - responsibility: register execution telemetry and audit events.
' - inputs: execution status, performance metrics, auth/scope context.
' - outputs: structured log payloads and telemetry registrations.
'
' UI
' - responsibility: host stateless interaction controls.
' - inputs: render payloads and current scope context.
' - outputs: user actions that trigger scoped runtime submissions.
'
'------------------------------------------------------------------------------
' ENGINE EXECUTION CONTEXT
'------------------------------------------------------------------------------
' The engine runtime executes using Auth* identity plus Effective* scoped
' contracts, then resolves Engine* execution parameters per action type.
' UI requests are treated as stateless submissions that rebuild context
' on each request before orchestration, SQL execution, rendering, and logging.
'
'------------------------------------------------------------------------------
' QUERY SAFETY GUARDRAILS
'------------------------------------------------------------------------------
' SQL must remain strictly SELECT-only and pass safety validation before run.
' SQL templates must consume Effective* scope parameters only, never direct
' Session state. Permission enforcement must complete before any SQL execution.
'
'------------------------------------------------------------------------------
' SQL OUTPUT DATA CONTRACT
'------------------------------------------------------------------------------
' SQL outputs must return tabular datasets consumable by the render pipeline:
' main table dataset, optional filter datasets, KPI datasets, and graph datasets.
' Dataset shape must remain deterministic for stable render and export behavior.
'
'------------------------------------------------------------------------------
' EXECUTION TELEMETRY
'------------------------------------------------------------------------------
' Engine orchestration must log execution start/end, status, and duration.
' Telemetry must include key auth/scope/report context and rows returned for
' governance traceability. UI remains stateless and does not own log state.
'
'------------------------------------------------------------------------------
' CORE RULES
'------------------------------------------------------------------------------
' 1. SQL queries must remain strictly read-only (SELECT only).
' 2. SQL must consume Effective* scope parameters only.
' 3. Permission enforcement must occur before SQL execution.
' 4. Engine layer is responsible for execution logging.
' 5. UI layer must remain stateless.
'
'------------------------------------------------------------------------------
' EXECUTION FLOW
'------------------------------------------------------------------------------
' 1. Resolve Auth user from Flowww session.
' 2. Compute Effective execution scope.
' 3. Load report configuration.
' 4. Load SQL template.
' 5. Inject engine parameters.
' 6. Execute SQL query.
' 7. Render result dataset.
' 8. Register execution telemetry.
'
'------------------------------------------------------------------------------
' CONTRACTS
'------------------------------------------------------------------------------
' SQL scope contract fields:
' - EffectiveUserID
' - EffectiveClinicIDs
' - StartDate
' - EndDate
' - Filter1
' - Filter2
' - Filter3
' SQL must not access Session variables directly.
' Naming contract:
' - Auth* => authenticated Flowww session context.
' - Effective* => permission-filtered execution scope.
' - Engine* => execution parameters and runtime control.
' - Render* => dataset rendering variables.
' - Log* => execution telemetry and logging variables.
'
'------------------------------------------------------------------------------
' GLOBAL CONFIGURATION
'------------------------------------------------------------------------------
' Engine constants and hard governance caps for runtime execution.
Const GM_LOG_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzi6kk52zAATTBbzWqgm55VDgYgmbpa1cAqrGjTpl8t2DN-HMwCmuRP1DYHNb7Mnmm56w/exec"
Const GM_ENGINE_VERSION = "GM_ENGINE_V1"
Const GM_MASTER_CONFIG_DEFAULT_URL = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_MASTER_CONFIG.json"
Const GM_SQL_TIMEOUT_HARD_CAP_SECONDS = 15
Const GM_SQL_MAX_ROWS_HARD_CAP = 50000

'------------------------------------------------------------------------------
' RUNTIME VARIABLES
'------------------------------------------------------------------------------
' Runtime state grouped by architecture domains.
'
' Auth* : authenticated Flowww session context.
Dim AuthSessionID, AuthSessionStart, AuthSessionTZ, AuthUserID, AuthUserName, AuthProfileID, AuthProfileDesc
Dim AuthClinicID, AuthClinicName, AuthPermClinicsIDs, AuthAllowedClinicIDs, AuthAllowedReportIDs
Dim AuthIsSuperAdmin
Dim sFlowwwHost
Dim sCurrentDBName, sConnStringRaw
Dim sDbInfoSQL, sDbVersionSQL, sDbVersion
Dim rsDbInfo, rsDbVersion
Dim sSessionUid, sSessionUnm, sSessionUlc, sSessionUcd, sSessionCid
Dim sSessionSda, sSessionSid, sSessionCtz, sSessionUip, sSessionUcl, sSessionUlw
Dim sCtxClientNow, sCtxClientTZ
Dim vCtxClientNow, vAuthSessionStart
Dim nCtxClientDeltaSec
Dim sMetaIPMasked
Dim rsSessionMeta, rsClinicLookup

' Effective* : permission-filtered execution scope.
Dim EffectiveUserID, EffectiveUserName, CtxUserName, EffectiveProfileID, EffectiveProfileDesc
Dim EffectiveAllowedReportIDs, EffectiveAllowedClinicIDs, EffectiveAllowedMinDate, EffectiveAllowedMaxDate
Dim EffectiveAllowedMinDateISO, EffectiveAllowedMaxDateISO
Dim sSelectedUserFilter, sUserFilterHTML, sUsersSQL
Dim uidUser, unameUser, selectedUserAttr, sFirstUserID, sFirstUserName
Dim bSelectedUserFound
Dim rsUsers, rsSelectedUser

' Engine* : execution parameters and runtime control.
Dim EngineName, EngineConfigName, EngineTitle, EngineAllowedClinicIDs, EngineReportCount
Dim PolicyLink_SKG, OperationsWA, SupportWA, DisclaimerText1, DisclaimerText2, AuthorText
Dim Report_Config_File
Dim ReportID, ReportParentCode, ReportParentName, ReportDisplayOrder, ReportName, ReportTitle
Dim ReportAllowedProfileIDs, ReportVersion, ReportIsActive, ReportNeedClinics, ReportNeedDates
Dim ReportHasFilters, ReportFiltersSqlFile, ReportFiltersDefaultValues
Dim ReportDataHorizontalScroll, ReportDataSumPerRow, ReportDataSumPerColumn, ReportDataSqlFile
Dim ReportHasKPIs, ReportKPIsSqlFile, ReportHasGraph, ReportGraphConfig, ReportGraphSqlFile
Dim sSQL
Dim sReportOptionsHTML, sSelectedReportID
Dim sStartDate, sEndDate
Dim sSelectedClinic, sClinicHTML, sAllClinicIDs
Dim cid, cname, selectedAttr
Dim bSelectedClinicFound
Dim CtxClinicIDs
Dim rsClinics
Dim ScopeBase, ScopeFull
Dim sReqFilter1IDs, sReqFilter2IDs, sReqFilter3IDs
Dim sFilter1ID, sFilter2ID, sFilter3ID
Dim sFilter1IDs, sFilter2IDs, sFilter3IDs
Dim sFilter1Title, sFilter2Title, sFilter3Title
Dim sFilter1Name, sFilter2Name, sFilter3Name
Dim sFilter1OptionsHTML, sFilter2OptionsHTML, sFilter3OptionsHTML
Dim bIsExportRequested
Dim bShowSqlCodeLink, bIsSqlPopupRequested, bSqlPopupAutoOpen, bIsSqlPopupApiRequested
Dim bIsConfigPopupRequested, bConfigPopupAutoOpen, bIsConfigPopupApiRequested
Dim sSqlPopupKind, sConfigPopupKind
Dim bIsDevOnlyPopupFlow
Dim nLoaderSafetyTimeoutMs
Dim MasterConfigJsonText
Dim MasterSecuritySuperAdminUserIDs, MasterSecurityDeveloperToolUserIDs, MasterSecuritySuperAdminProfileIDs
Dim MasterSecurityDeveloperUserIDs
Dim MasterDateStandardUserMinMonthOffset, MasterDateStandardUserMaxMonthOffset
Dim MasterDateSuperAdminMinDateISO, MasterDateSuperAdminMaxMonthOffset
Dim MasterEngineDefaultDateRangeDays, MasterEngineActive
Dim MasterCacheEnable, MasterCacheConfigTTLSeconds, MasterCacheReportsTTLSeconds
Dim MasterPerfHttpFetchTimeoutMs, MasterPerfHttpLogTimeoutMs, MasterPerfSlowQueryThresholdMs, MasterPerfSlowRenderThresholdMs
Dim MasterSqlTimeoutSeconds, MasterSqlMaxRows
Dim MasterKpiTimeoutMs, MasterKpiMaxItems, MasterGraphTimeoutMs, MasterGraphMaxPoints
Dim MasterEngineEnvironment, MasterEngineEnableHomeDashboard, MasterEngineCorporateClinicOrder
Dim PathLogEndpointValue, EngineVersionValue, PathMasterConfigValue, PathModulesConfigValue, PathHomeConfigValue
Dim PathReportsFolderValue, PathIconsFolderValue, PathLoaderIconValue, PathMaintenanceIconValue
Dim UIFooterDisclaimerText3Value
Dim MsgStdMaintenanceTitle, MsgStdMaintenanceBody, MsgStdMaintenanceRetry
Dim MsgStdReportUndefined, MsgStdReportUnavailable, MsgStdTableNoData, MsgStdKpiNoData
Dim MsgStdGraphNoData, MsgStdGraphSqlBuildFailed, MsgStdGraphLibLoadFailed, MsgStdGraphCanvasInitFailed
Dim MsgStdSqlNotSafe, MsgStdSqlPlaceholdersUnresolved, MsgStdSqlEmptyAfterBuild, MsgStdNotAvailable, MsgStdNA
Dim MsgAdmNoActiveUsers, MsgAdmLogLastActionTitle
Dim MsgDevFiltersNotConfigured, MsgDevDataSqlNotConfigured, MsgDevKpisNotConfigured, MsgDevGraphNotConfigured, MsgDevSqlEmptyRender
Dim MsgDevSqlLoading, MsgDevSqlPopupErrorTitle, MsgDevSqlPopupErrorBody, MsgDevConfigLoading, MsgDevConfigPopupErrorTitle, MsgDevConfigPopupErrorBody
Dim WAOpsTemplateValue, WASupportTemplateValue

' Render* : dataset rendering variables and UI payload state.
Dim rsData, rsKPIs, rsGraph
Dim CtxCSSpx, CtxTimeStamp
Dim TxtMetadatos
Dim sWaOperacionesUrl, sWaSoporteUrl, sWaOperacionesMsg, sWaSoporteMsg
Dim GraphDataJson, GraphConfigJson
Dim SqlPopupUrl, SqlPopupRaw, SqlPopupClean, SqlPopupContent, SqlPopupStatusIcon, SqlPopupBuildStatus, SqlPopupBuildError
Dim ConfigPopupUrl, ConfigPopupContent, ConfigPopupStatusIcon, ConfigPopupBuildStatus, ConfigPopupBuildError
Dim gReportConfigValues, gReportConfigMaxID

' Log* : execution telemetry and timing state.
Dim TxtWebhookPayload, TxtLogExecutionTimestamp
Dim ExecutionTimeMs, t0, t1, RequestStartTimer
'------------------------------------------------------------------------------
' HELPER UTILITIES
'------------------------------------------------------------------------------
' Generic parsing, normalization, and safe utility functions used by the engine.
Function ToLongOrZero(v)
    If IsNumeric(v) Then
        ToLongOrZero = CLng(v)
    Else
        ToLongOrZero = 0
    End If
End Function

Function GM_EscapeForJsSingleQuoted(v)
    Dim s
    s = CStr(v)
    s = Replace(s, "\", "\\")
    s = Replace(s, "'", "\'")
    s = Replace(s, vbCrLf, "\n")
    s = Replace(s, vbCr, "\n")
    s = Replace(s, vbLf, "\n")
    GM_EscapeForJsSingleQuoted = s
End Function

Sub GM_EmitLoaderPhase(phaseCode)
    ' No-op in buffered/proxied environments.
    ' Kept for backward compatibility with old streaming flow.
End Sub

Sub GM_EmitLoaderBootstrap(loaderLogoUrl, timeoutMs)
    ' No-op in buffered/proxied environments.
    ' Loader bootstrap now runs client-side before submit.
End Sub


Function Gm_OpenRs(sqlText)
    Dim rsTmp, sqlTimeoutSec
    Set Gm_OpenRs = Nothing
    If Len(Trim(CStr(sqlText))) = 0 Then Exit Function

    Set rsTmp = Server.CreateObject("ADODB.Recordset")
    On Error Resume Next
    sqlTimeoutSec = ToLongOrZero(MasterSqlTimeoutSeconds)
    If sqlTimeoutSec > 0 Then
        If Not objConnection Is Nothing Then objConnection.CommandTimeout = sqlTimeoutSec
    End If
    Err.Clear
    rsTmp.Open sqlText, objConnection, adOpenForwardOnly, adLockReadOnly
    If Err.Number = 0 Then
        Set Gm_OpenRs = rsTmp
    Else
        Err.Clear
        If Not rsTmp Is Nothing Then
            If rsTmp.State = 1 Then rsTmp.Close
            Set rsTmp = Nothing
        End If
    End If
    On Error GoTo 0
End Function


Function GetServerVarText(keyName)
    Dim v
    v = ""
    On Error Resume Next
    v = CStr(Request.ServerVariables(keyName))
    If Err.Number <> 0 Then
        v = ""
        Err.Clear
    End If
    On Error GoTo 0

    If InStr(1, v, "[object:IStringList]", vbTextCompare) > 0 Then v = ""
    GetServerVarText = Trim(v)
End Function

Function GetConnStringValue(connStr, targetKey)
    Dim parts, i, part, sepPos, keyName, keyValue
    GetConnStringValue = ""
    If Len(Trim(connStr)) = 0 Then Exit Function

    parts = Split(connStr, ";")
    For i = 0 To UBound(parts)
        part = Trim(parts(i))
        If Len(part) > 0 Then
            sepPos = InStr(1, part, "=", vbTextCompare)
            If sepPos > 1 Then
                keyName = UCase(Trim(Left(part, sepPos - 1)))
                keyValue = Trim(Mid(part, sepPos + 1))
                If keyName = UCase(targetKey) Then
                    GetConnStringValue = keyValue
                    Exit Function
                End If
            End If
        End If
    Next
End Function


Function ParseDateTimeFlexibleOrBlank(v)
    Dim t, parsedDate
    ParseDateTimeFlexibleOrBlank = ""
    t = Trim(CStr(v))
    If Len(t) = 0 Then Exit Function

    If Len(t) >= 19 Then
        If Mid(t, 5, 1) = "-" And Mid(t, 8, 1) = "-" And (Mid(t, 11, 1) = " " Or Mid(t, 11, 1) = "T") And Mid(t, 14, 1) = ":" And Mid(t, 17, 1) = ":" Then
            If IsNumeric(Left(t, 4)) And IsNumeric(Mid(t, 6, 2)) And IsNumeric(Mid(t, 9, 2)) And IsNumeric(Mid(t, 12, 2)) And IsNumeric(Mid(t, 15, 2)) And IsNumeric(Mid(t, 18, 2)) Then
                On Error Resume Next
                parsedDate = DateSerial(CLng(Left(t, 4)), CLng(Mid(t, 6, 2)), CLng(Mid(t, 9, 2))) + TimeSerial(CLng(Mid(t, 12, 2)), CLng(Mid(t, 15, 2)), CLng(Mid(t, 18, 2)))
                If Err.Number = 0 Then
                    ParseDateTimeFlexibleOrBlank = parsedDate
                    On Error GoTo 0
                    Exit Function
                End If
                Err.Clear
                On Error GoTo 0
            End If
        ElseIf Mid(t, 3, 1) = "/" And Mid(t, 6, 1) = "/" And Mid(t, 11, 1) = " " And Mid(t, 14, 1) = ":" And Mid(t, 17, 1) = ":" Then
            If IsNumeric(Left(t, 2)) And IsNumeric(Mid(t, 4, 2)) And IsNumeric(Mid(t, 7, 4)) And IsNumeric(Mid(t, 12, 2)) And IsNumeric(Mid(t, 15, 2)) And IsNumeric(Mid(t, 18, 2)) Then
                On Error Resume Next
                parsedDate = DateSerial(CLng(Mid(t, 7, 4)), CLng(Mid(t, 4, 2)), CLng(Left(t, 2))) + TimeSerial(CLng(Mid(t, 12, 2)), CLng(Mid(t, 15, 2)), CLng(Mid(t, 18, 2)))
                If Err.Number = 0 Then
                    ParseDateTimeFlexibleOrBlank = parsedDate
                    On Error GoTo 0
                    Exit Function
                End If
                Err.Clear
                On Error GoTo 0
            End If
        End If
    End If

    On Error Resume Next
    If IsDate(t) Then ParseDateTimeFlexibleOrBlank = CDate(t)
    Err.Clear
    On Error GoTo 0
End Function

Function FormatToDMYDateTime(v)
    Dim t
    t = Trim(CStr(v))
    If Len(t) >= 19 And Mid(t, 5, 1) = "-" And Mid(t, 8, 1) = "-" Then
        FormatToDMYDateTime = Mid(t, 9, 2) & "/" & Mid(t, 6, 2) & "/" & Left(t, 4) & " " & Mid(t, 12, 8)
    Else
        FormatToDMYDateTime = t
    End If
    If Len(Trim(FormatToDMYDateTime)) = 0 Then FormatToDMYDateTime = "-"
End Function


Function IsValidIPv4(ip)
    Dim parts, k, octet
    IsValidIPv4 = False
    ip = Trim(CStr(ip))
    If Len(ip) = 0 Then Exit Function
    parts = Split(ip, ".")
    If UBound(parts) <> 3 Then Exit Function

    For k = 0 To 3
        If Not IsNumeric(parts(k)) Then Exit Function
        octet = CLng(parts(k))
        If octet < 0 Or octet > 255 Then Exit Function
    Next
    IsValidIPv4 = True
End Function


Function MaskIPv4LastOctet(ip)
    Dim parts
    ip = Trim(CStr(ip))
    If IsValidIPv4(ip) Then
        parts = Split(ip, ".")
        MaskIPv4LastOctet = parts(0) & "." & parts(1) & "." & parts(2) & ".xxx"
    Else
        MaskIPv4LastOctet = ip
    End If
End Function

Function FirstIPToken(ip)
    Dim tmp
    tmp = Trim(CStr(ip))
    If InStr(1, tmp, ",", vbTextCompare) > 0 Then
        tmp = Trim(Split(tmp, ",")(0))
    End If
    FirstIPToken = tmp
End Function

'------------------------------------------------------------------------------
' AUTH RESOLUTION
'------------------------------------------------------------------------------
' Functions used to resolve and validate authenticated identity context.

Function IsSuperAdminCode(v)
    Dim t, profileIDsCSV
    t = Trim(CStr(v))
    IsSuperAdminCode = False
    If Len(t) = 0 Then Exit Function

    If Not IsNumeric(t) Then Exit Function
    profileIDsCSV = CsvNormalizeIntList(MasterSecuritySuperAdminProfileIDs)
    If Len(profileIDsCSV) = 0 Then profileIDsCSV = "0,1,6,16"
    IsSuperAdminCode = CsvContainsInt(profileIDsCSV, CLng(t))
End Function


Function CsvNormalizeIntList(csvText)
    Dim rawText, parts, i, token, outList, tokenNum
    rawText = Trim(CStr(csvText))
    outList = ""
    If Len(rawText) = 0 Then
        CsvNormalizeIntList = ""
        Exit Function
    End If
    parts = Split(rawText, ",")
    For i = 0 To UBound(parts)
        token = Trim(parts(i))
        If Len(token) > 0 And IsNumeric(token) Then
            tokenNum = CStr(CLng(token))
            If InStr(1, "," & outList & ",", "," & tokenNum & ",", vbTextCompare) = 0 Then
                If Len(outList) > 0 Then outList = outList & ","
                outList = outList & tokenNum
            End If
        End If
    Next
    CsvNormalizeIntList = outList
End Function


Function CsvContainsInt(csvText, intValue)
    Dim normText, target
    normText = CsvNormalizeIntList(csvText)
    target = CStr(ToLongOrZero(intValue))
    CsvContainsInt = (InStr(1, "," & normText & ",", "," & target & ",", vbTextCompare) > 0)
End Function


Function CsvIntersect(baseList, allowedList)
    Dim normBase, normAllowed, parts, i, token, outList
    normBase = CsvNormalizeIntList(baseList)
    normAllowed = CsvNormalizeIntList(allowedList)
    outList = ""
    If Len(normBase) = 0 Or Len(normAllowed) = 0 Then
        CsvIntersect = ""
        Exit Function
    End If
    parts = Split(normBase, ",")
    For i = 0 To UBound(parts)
        token = Trim(parts(i))
        If Len(token) > 0 Then
            If InStr(1, "," & normAllowed & ",", "," & token & ",", vbTextCompare) > 0 Then
                If Len(outList) > 0 Then outList = outList & ","
                outList = outList & token
            End If
        End If
    Next
    CsvIntersect = CsvNormalizeIntList(outList)
End Function


Function CsvFirst(csvText, fallbackText)
    Dim normText
    normText = CsvNormalizeIntList(csvText)
    If Len(normText) = 0 Then
        CsvFirst = fallbackText
        Exit Function
    End If
    CsvFirst = Trim(Split(normText, ",")(0))
End Function

Function GM_CsvFromRequest(keyName)
    GM_CsvFromRequest = CsvNormalizeIntList(Request(CStr(keyName)))
End Function

Function GM_RecordsetFieldTextOrBlank(rsObj, fieldName)
    Dim v
    GM_RecordsetFieldTextOrBlank = ""
    If rsObj Is Nothing Then Exit Function

    On Error Resume Next
    v = rsObj(CStr(fieldName))
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    If IsNull(v) Then Exit Function
    GM_RecordsetFieldTextOrBlank = Trim(CStr(v))
End Function


'------------------------------------------------------------------------------
' SCOPE COMPUTATION
'------------------------------------------------------------------------------
' Computes Effective* scope and permission-governed execution boundaries.
Function IsReportAllowedForProfile(profileID, allowedProfilesCSV)
    IsReportAllowedForProfile = CsvContainsInt(allowedProfilesCSV, profileID)
End Function


Function GM_IsHex4(hexText)
    Dim re
    GM_IsHex4 = False
    If Len(CStr(hexText)) <> 4 Then Exit Function

    On Error Resume Next
    Set re = Server.CreateObject("VBScript.RegExp")
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    re.Global = False
    re.IgnoreCase = True
    re.MultiLine = False
    re.Pattern = "^[0-9a-fA-F]{4}$"
    GM_IsHex4 = re.Test(CStr(hexText))
    Set re = Nothing
End Function


Function GM_UnescapeJsonString(v)
    Dim s, outText, i, ch, esc, hexCode
    s = CStr(v)
    outText = ""
    i = 1

    Do While i <= Len(s)
        ch = Mid(s, i, 1)
        If ch = "\" And i < Len(s) Then
            esc = Mid(s, i + 1, 1)
            Select Case esc
                Case """"
                    outText = outText & """"
                    i = i + 2
                Case "\"
                    outText = outText & "\"
                    i = i + 2
                Case "/"
                    outText = outText & "/"
                    i = i + 2
                Case "b"
                    outText = outText & Chr(8)
                    i = i + 2
                Case "f"
                    outText = outText & Chr(12)
                    i = i + 2
                Case "n"
                    outText = outText & vbLf
                    i = i + 2
                Case "r"
                    outText = outText & vbCr
                    i = i + 2
                Case "t"
                    outText = outText & vbTab
                    i = i + 2
                Case "u", "U"
                    If i + 5 <= Len(s) Then
                        hexCode = Mid(s, i + 2, 4)
                        If GM_IsHex4(hexCode) Then
                            outText = outText & ChrW(CLng("&H" & hexCode))
                            i = i + 6
                        Else
                            outText = outText & ch
                            i = i + 1
                        End If
                    Else
                        outText = outText & ch
                        i = i + 1
                    End If
                Case Else
                    outText = outText & esc
                    i = i + 2
            End Select
        Else
            outText = outText & ch
            i = i + 1
        End If
    Loop

    GM_UnescapeJsonString = outText
End Function


Function GM_JsonGetString(objText, keyName)
    Dim q, pattern, re, matches, rawToken, rawValue
    GM_JsonGetString = ""
    q = Chr(34)
    pattern = q & CStr(keyName) & q & "\s*:\s*(null|" & q & "((?:[^" & q & "\\]|\\.)*)" & q & ")"

    On Error Resume Next
    Set re = Server.CreateObject("VBScript.RegExp")
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    re.Global = False
    re.IgnoreCase = True
    re.MultiLine = True
    re.Pattern = pattern

    Set matches = re.Execute(CStr(objText))
    If matches.Count > 0 Then
        rawToken = LCase(Trim(CStr(matches(0).SubMatches(0))))
        If rawToken = "null" Then
            GM_JsonGetString = ""
        Else
            rawValue = CStr(matches(0).SubMatches(1))
            GM_JsonGetString = GM_UnescapeJsonString(rawValue)
        End If
    End If

    Set matches = Nothing
    Set re = Nothing
End Function


Function GM_JsonGetNumberText(objText, keyName, defaultText)
    Dim q, pattern, re, matches
    GM_JsonGetNumberText = CStr(defaultText)
    q = Chr(34)
    pattern = q & CStr(keyName) & q & "\s*:\s*(-?\d+)"

    On Error Resume Next
    Set re = Server.CreateObject("VBScript.RegExp")
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    re.Global = False
    re.IgnoreCase = True
    re.MultiLine = True
    re.Pattern = pattern

    Set matches = re.Execute(CStr(objText))
    If matches.Count > 0 Then
        GM_JsonGetNumberText = CStr(matches(0).SubMatches(0))
    End If

    Set matches = Nothing
    Set re = Nothing
End Function


Function GM_JsonGetBool01(objText, keyName, defaultText)
    Dim q, pattern, re, matches, rawToken
    GM_JsonGetBool01 = CStr(defaultText)
    q = Chr(34)
    pattern = q & CStr(keyName) & q & "\s*:\s*(true|false)"

    On Error Resume Next
    Set re = Server.CreateObject("VBScript.RegExp")
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    re.Global = False
    re.IgnoreCase = True
    re.MultiLine = True
    re.Pattern = pattern

    Set matches = re.Execute(CStr(objText))
    If matches.Count > 0 Then
        rawToken = LCase(Trim(CStr(matches(0).SubMatches(0))))
        If rawToken = "true" Then
            GM_JsonGetBool01 = "1"
        ElseIf rawToken = "false" Then
            GM_JsonGetBool01 = "0"
        End If
    End If

    Set matches = Nothing
    Set re = Nothing
End Function


Function GM_JsonGetIntArrayCsv(objText, keyName, defaultText)
    Dim q, pattern, re, matches, rawList
    GM_JsonGetIntArrayCsv = CStr(defaultText)
    q = Chr(34)
    pattern = q & CStr(keyName) & q & "\s*:\s*\[([^\]]*)\]"

    On Error Resume Next
    Set re = Server.CreateObject("VBScript.RegExp")
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    re.Global = False
    re.IgnoreCase = True
    re.MultiLine = True
    re.Pattern = pattern

    Set matches = re.Execute(CStr(objText))
    If matches.Count > 0 Then
        rawList = CStr(matches(0).SubMatches(0))
        rawList = Replace(rawList, vbCrLf, "")
        rawList = Replace(rawList, vbCr, "")
        rawList = Replace(rawList, vbLf, "")
        rawList = Replace(rawList, vbTab, "")
        rawList = Replace(rawList, " ", "")
        GM_JsonGetIntArrayCsv = CsvNormalizeIntList(rawList)
    End If

    Set matches = Nothing
    Set re = Nothing
End Function


Sub GM_SetReportConfigValue(reportId, keyName, valueText)
    Dim dictKey
    If Not IsObject(gReportConfigValues) Then Exit Sub
    dictKey = CStr(ToLongOrZero(reportId)) & "|" & UCase(Trim(CStr(keyName)))
    gReportConfigValues(dictKey) = CStr(valueText)
End Sub


Sub GM_AddReportConfigFromJsonObject(objText)
    Dim rid, parentCode, parentName, reportName, reportTitle
    Dim allowedProfilesCSV, reportDisplayOrder

    rid = ToLongOrZero(GM_JsonGetNumberText(objText, "ReportID", "0"))
    If rid <= 0 Then Exit Sub

    If rid > ToLongOrZero(gReportConfigMaxID) Then gReportConfigMaxID = rid

    parentCode = GM_JsonGetString(objText, "ReportParentCode")
    parentName = GM_JsonGetString(objText, "ReportParentName")
    If Len(Trim(parentName)) = 0 And UCase(Trim(parentCode)) = "CATALOGS" Then parentName = "Catalogos"

    reportDisplayOrder = GM_JsonGetNumberText(objText, "ReportDisplayOrder", CStr(rid))
    reportName = GM_JsonGetString(objText, "ReportName")
    reportTitle = GM_JsonGetString(objText, "ReportTitle")
    If Len(Trim(reportTitle)) = 0 Then reportTitle = reportName
    allowedProfilesCSV = GM_JsonGetIntArrayCsv(objText, "ReportAllowedProfileIDs", "")

    Call GM_SetReportConfigValue(rid, "ReportID", CStr(rid))
    Call GM_SetReportConfigValue(rid, "ReportParentCode", parentCode)
    Call GM_SetReportConfigValue(rid, "ReportParentName", parentName)
    Call GM_SetReportConfigValue(rid, "ReportDisplayOrder", reportDisplayOrder)
    Call GM_SetReportConfigValue(rid, "ReportName", reportName)
    Call GM_SetReportConfigValue(rid, "ReportTitle", reportTitle)
    Call GM_SetReportConfigValue(rid, "ReportAllowedProfileIDs", allowedProfilesCSV)
    Call GM_SetReportConfigValue(rid, "ReportVersion", GM_JsonGetString(objText, "ReportVersion"))
    Call GM_SetReportConfigValue(rid, "ReportIsActive", GM_JsonGetBool01(objText, "ReportIsActive", "0"))
    Call GM_SetReportConfigValue(rid, "ReportNeedClinics", GM_JsonGetBool01(objText, "ReportNeedClinics", "0"))
    Call GM_SetReportConfigValue(rid, "ReportNeedDates", GM_JsonGetBool01(objText, "ReportNeedDates", "0"))
    Call GM_SetReportConfigValue(rid, "ReportHasFilters", GM_JsonGetBool01(objText, "ReportHasFilters", "0"))
    Call GM_SetReportConfigValue(rid, "ReportFiltersSqlFile", GM_JsonGetString(objText, "ReportFiltersSqlFile"))
    Call GM_SetReportConfigValue(rid, "ReportFiltersDefaultValues", GM_JsonGetString(objText, "ReportFiltersDefaultValues"))
    Call GM_SetReportConfigValue(rid, "ReportDataHorizontalScroll", GM_JsonGetBool01(objText, "ReportDataHorizontalScroll", "0"))
    Call GM_SetReportConfigValue(rid, "ReportDataSumPerRow", GM_JsonGetBool01(objText, "ReportDataSumPerRow", "0"))
    Call GM_SetReportConfigValue(rid, "ReportDataSumPerColumn", GM_JsonGetBool01(objText, "ReportDataSumPerColumn", "0"))
    Call GM_SetReportConfigValue(rid, "ReportDataSqlFile", GM_JsonGetString(objText, "ReportDataSqlFile"))
    Call GM_SetReportConfigValue(rid, "ReportHasKPIs", GM_JsonGetBool01(objText, "ReportHasKPIs", "0"))
    Call GM_SetReportConfigValue(rid, "ReportKPIsSqlFile", GM_JsonGetString(objText, "ReportKPIsSqlFile"))
    Call GM_SetReportConfigValue(rid, "ReportHasGraph", GM_JsonGetBool01(objText, "ReportHasGraph", "0"))
    Call GM_SetReportConfigValue(rid, "ReportGraphConfig", GM_JsonGetString(objText, "ReportGraphConfig"))
    Call GM_SetReportConfigValue(rid, "ReportGraphSqlFile", GM_JsonGetString(objText, "ReportGraphSqlFile"))
End Sub


Sub GM_PlatformMaintenance()
    Dim mtTitle, mtBody, mtRetry, maintenanceIcon
    mtTitle = Trim(CStr(MsgStdMaintenanceTitle))
    mtBody = Trim(CStr(MsgStdMaintenanceBody))
    mtRetry = Trim(CStr(MsgStdMaintenanceRetry))
    maintenanceIcon = Trim(CStr(PathMaintenanceIconValue))
    If Len(mtTitle) = 0 Then mtTitle = "Plataforma en mantenimiento"
    If Len(mtBody) = 0 Then mtBody = "Estamos actualizando la configuracion del sistema."
    If Len(mtRetry) = 0 Then mtRetry = "Intente nuevamente en unos minutos."

    Response.Clear
    Response.Status = "503 Service Unavailable"
    Response.Charset = "windows-1252"
    Response.ContentType = "text/html"
    Response.Write "<!DOCTYPE html>"
    Response.Write "<html><head><meta charset=""windows-1252""><title>" & Server.HTMLEncode(mtTitle) & "</title>"
    Response.Write "<style>body{font-family:Arial;background:#f5f5f5;text-align:center;padding-top:120px;color:#333}.box{background:#fff;padding:40px;width:400px;margin:auto;border-radius:6px;box-shadow:0 2px 10px rgba(0,0,0,.1)}h1{font-size:20px;margin-bottom:10px}p{font-size:14px;color:#666}.icon{max-width:96px;height:auto;margin:0 auto 14px auto;display:block}</style>"
    Response.Write "</head><body><div class=""box"">"
    If Len(maintenanceIcon) > 0 Then Response.Write "<img class=""icon"" src=""" & Server.HTMLEncode(maintenanceIcon) & """ alt=""mantenimiento"">"
    Response.Write "<h1>" & Server.HTMLEncode(mtTitle) & "</h1><p>" & Server.HTMLEncode(mtBody) & "</p><p>" & Server.HTMLEncode(mtRetry) & "</p></div></body></html>"
End Sub


Function GM_IsValidReportsConfigJson(jsonText)
    Dim t
    t = Trim(CStr(jsonText))
    GM_IsValidReportsConfigJson = False
    If Len(t) = 0 Then Exit Function
    If Left(t, 1) <> "[" Then Exit Function
    If InStr(1, t, """ReportID""", vbTextCompare) = 0 Then Exit Function
    GM_IsValidReportsConfigJson = True
End Function


Function GM_GetReportsConfigJson()
    Dim configUrl, cachedJson, cachedAtText, cacheEnabled, cacheTtlSec, cacheAgeSec
    Dim http, jsonText, fetchTimeoutMs
    configUrl = Trim(CStr(Report_Config_File))
    If Len(configUrl) = 0 Then configUrl = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_REPORT_CONFIG.json"

    cacheEnabled = CBool(MasterCacheEnable)
    cacheTtlSec = ToLongOrZero(MasterCacheReportsTTLSeconds)
    fetchTimeoutMs = ToLongOrZero(MasterPerfHttpFetchTimeoutMs)
    If fetchTimeoutMs <= 0 Then fetchTimeoutMs = 5000

    On Error Resume Next
    cachedJson = ""
    cachedAtText = ""
    If cacheEnabled Then
        cachedJson = Trim(CStr(Session("GM_Config_JSON")))
        cachedAtText = Trim(CStr(Session("GM_Config_JSON_TS")))
    End If
    If Err.Number <> 0 Then
        cachedJson = ""
        cachedAtText = ""
        Err.Clear
    End If
    On Error GoTo 0

    If cacheEnabled Then
        If Len(cachedJson) > 0 And GM_IsValidReportsConfigJson(cachedJson) Then
            If cacheTtlSec <= 0 Then
                GM_GetReportsConfigJson = cachedJson
                Exit Function
            End If

            If IsDate(cachedAtText) Then
                cacheAgeSec = DateDiff("s", CDate(cachedAtText), Now())
                If cacheAgeSec >= 0 And cacheAgeSec <= cacheTtlSec Then
                    GM_GetReportsConfigJson = cachedJson
                    Exit Function
                End If
            Else
                ' Backward compatibility for sessions created before cache timestamp key existed.
                GM_GetReportsConfigJson = cachedJson
                Exit Function
            End If
        End If
    End If

    On Error Resume Next
    Set http = Server.CreateObject("MSXML2.ServerXMLHTTP.6.0")
    If Err.Number <> 0 Then
        Err.Clear
        Set http = Server.CreateObject("MSXML2.ServerXMLHTTP")
    End If

    If Err.Number <> 0 Or (http Is Nothing) Then
        Err.Clear
        On Error GoTo 0
        Call GM_PlatformMaintenance()
        Response.End
    End If

    http.Open "GET", configUrl, False
    http.setTimeouts fetchTimeoutMs, fetchTimeoutMs, fetchTimeoutMs, fetchTimeoutMs
    http.setRequestHeader "Cache-Control", "no-cache"
    http.Send

    If Err.Number <> 0 Then
        Err.Clear
        Set http = Nothing
        On Error GoTo 0
        Call GM_PlatformMaintenance()
        Response.End
    End If

    If CLng(http.Status) <> 200 Then
        Set http = Nothing
        On Error GoTo 0
        Call GM_PlatformMaintenance()
        Response.End
    End If

    jsonText = CStr(http.responseText)
    Set http = Nothing
    On Error GoTo 0

    If Not GM_IsValidReportsConfigJson(jsonText) Then
        Call GM_PlatformMaintenance()
        Response.End
    End If

    If cacheEnabled Then
        Session("GM_Config_JSON") = jsonText
        Session("GM_Config_JSON_TS") = CStr(Now())
    End If
    GM_GetReportsConfigJson = jsonText
End Function


Sub GM_EnsureReportConfigCache()
    Dim jsonText, reObj, matches, i, objectText
    If IsObject(gReportConfigValues) Then Exit Sub

    Set gReportConfigValues = Server.CreateObject("Scripting.Dictionary")
    gReportConfigMaxID = 0
    jsonText = Trim(CStr(GM_GetReportsConfigJson()))

    If Len(jsonText) > 0 Then
        On Error Resume Next
        Set reObj = Server.CreateObject("VBScript.RegExp")
        If Err.Number = 0 Then
            reObj.Global = True
            reObj.IgnoreCase = True
            reObj.MultiLine = True
            reObj.Pattern = "\{[\s\S]*?\}"
            Set matches = reObj.Execute(jsonText)

            If Err.Number = 0 Then
                For i = 0 To matches.Count - 1
                    objectText = CStr(matches(i).Value)
                    Call GM_AddReportConfigFromJsonObject(objectText)
                Next
            End If
        End If
        Err.Clear
        On Error GoTo 0
        Set matches = Nothing
        Set reObj = Nothing
    End If

    If gReportConfigValues.Count = 0 Then
        Call GM_PlatformMaintenance()
        Response.End
    End If
End Sub


Function GM_GetReportConfigDefaultValue(reportId, keyName)
    Dim rid, key
    rid = ToLongOrZero(reportId)
    key = UCase(Trim(CStr(keyName)))
    GM_GetReportConfigDefaultValue = ""

    Select Case key
        Case "REPORTNAME", "REPORTTITLE"
            GM_GetReportConfigDefaultValue = CStr(MsgStdReportUndefined)
        Case "REPORTISACTIVE", "REPORTNEEDCLINICS", "REPORTNEEDDATES", "REPORTHASFILTERS", "REPORTDATAHORIZONTALSCROLL", "REPORTDATASUMPERROW", "REPORTDATASUMPERCOLUMN", "REPORTHASKPIS", "REPORTHASGRAPH"
            GM_GetReportConfigDefaultValue = "0"
        Case "REPORTID"
            GM_GetReportConfigDefaultValue = CStr(rid)
        Case "REPORTDISPLAYORDER"
            GM_GetReportConfigDefaultValue = CStr(rid)
    End Select
End Function


Function GM_GetReportConfigMaxID()
    Call GM_EnsureReportConfigCache()
    GM_GetReportConfigMaxID = ToLongOrZero(gReportConfigMaxID)
End Function


Function GetReportConfigValueByID(reportId, keyName)
    Dim rid, key, dictKey
    Call GM_EnsureReportConfigCache()

    rid = ToLongOrZero(reportId)
    key = UCase(Trim(CStr(keyName)))
    dictKey = CStr(rid) & "|" & key
    GetReportConfigValueByID = ""

    If IsObject(gReportConfigValues) Then
        If gReportConfigValues.Exists(dictKey) Then
            GetReportConfigValueByID = CStr(gReportConfigValues(dictKey))
            Exit Function
        End If
    End If

    GetReportConfigValueByID = GM_GetReportConfigDefaultValue(rid, key)
End Function


Function GetReportIsActiveByID(reportId)
    GetReportIsActiveByID = (ToLongOrZero(GetReportConfigValueByID(reportId, "ReportIsActive")) > 0)
End Function


Function GetEngineReportIDsCSV()
    Dim rid, reportName, outList
    outList = ""
    For rid = 1 To ToLongOrZero(EngineReportCount)
        reportName = GetReportNameByID(rid)
        If GetReportIsActiveByID(rid) And UCase(Trim(reportName)) <> UCase(CStr(MsgStdReportUndefined)) Then
            If Len(outList) > 0 Then outList = outList & ","
            outList = outList & CStr(rid)
        End If
    Next
    GetEngineReportIDsCSV = CsvNormalizeIntList(outList)
End Function

Function BuildAllowedReportIDsForProfile(profileID)
    Dim reportIDs, parts, i, rid, allowedProfiles, outList
    reportIDs = GetEngineReportIDsCSV()
    outList = ""
    If Len(reportIDs) = 0 Then
        BuildAllowedReportIDsForProfile = ""
        Exit Function
    End If

    parts = Split(reportIDs, ",")
    For i = 0 To UBound(parts)
        rid = Trim(parts(i))
        If Len(rid) > 0 Then
            allowedProfiles = GetReportAllowedProfileIDsByID(rid)
            If IsReportAllowedForProfile(profileID, allowedProfiles) Then
                If Len(outList) > 0 Then outList = outList & ","
                outList = outList & CStr(ToLongOrZero(rid))
            End If
        End If
    Next
    BuildAllowedReportIDsForProfile = CsvNormalizeIntList(outList)
End Function

'------------------------------------------------------------------------------
' ENGINE ORCHESTRATION
'------------------------------------------------------------------------------
' Resolves report metadata and engine-level configuration contracts.
Function GetReportNameByID(reportId)
    GetReportNameByID = CStr(GetReportConfigValueByID(reportId, "ReportName"))
    If Len(Trim(GetReportNameByID)) = 0 Then GetReportNameByID = CStr(MsgStdReportUndefined)
End Function

Function GetReportTitleByID(reportId)
    GetReportTitleByID = CStr(GetReportConfigValueByID(reportId, "ReportTitle"))
    If Len(Trim(GetReportTitleByID)) = 0 Then GetReportTitleByID = CStr(MsgStdReportUndefined)
End Function

Function GetReportAllowedProfileIDsByID(reportId)
    GetReportAllowedProfileIDsByID = CsvNormalizeIntList(GetReportConfigValueByID(reportId, "ReportAllowedProfileIDs"))
End Function


Function GM_IsValidMasterConfigJson(jsonText)
    Dim t
    t = Trim(CStr(jsonText))
    GM_IsValidMasterConfigJson = False
    If Len(t) = 0 Then Exit Function
    If Left(t, 1) <> "{" Then Exit Function
    If InStr(1, t, """EngineName""", vbTextCompare) = 0 Then Exit Function
    GM_IsValidMasterConfigJson = True
End Function


Function GM_GetMasterConfigJson()
    Dim configUrl, cachedJson, cachedAtText, cacheEnabled, cacheTtlSec, cacheAgeSec
    Dim http, jsonText, fetchTimeoutMs
    configUrl = Trim(CStr(GM_MASTER_CONFIG_DEFAULT_URL))
    If Len(configUrl) = 0 Then
        GM_GetMasterConfigJson = ""
        Exit Function
    End If

    cacheEnabled = CBool(MasterCacheEnable)
    cacheTtlSec = ToLongOrZero(MasterCacheConfigTTLSeconds)
    fetchTimeoutMs = ToLongOrZero(MasterPerfHttpFetchTimeoutMs)
    If fetchTimeoutMs <= 0 Then fetchTimeoutMs = 5000

    On Error Resume Next
    cachedJson = ""
    cachedAtText = ""
    If cacheEnabled Then
        cachedJson = Trim(CStr(Session("GM_MASTER_CONFIG_JSON")))
        cachedAtText = Trim(CStr(Session("GM_MASTER_CONFIG_JSON_TS")))
    End If
    If Err.Number <> 0 Then
        cachedJson = ""
        cachedAtText = ""
        Err.Clear
    End If
    On Error GoTo 0

    If cacheEnabled Then
        If Len(cachedJson) > 0 And GM_IsValidMasterConfigJson(cachedJson) Then
            If cacheTtlSec <= 0 Then
                GM_GetMasterConfigJson = cachedJson
                Exit Function
            End If

            If IsDate(cachedAtText) Then
                cacheAgeSec = DateDiff("s", CDate(cachedAtText), Now())
                If cacheAgeSec >= 0 And cacheAgeSec <= cacheTtlSec Then
                    GM_GetMasterConfigJson = cachedJson
                    Exit Function
                End If
            Else
                ' Backward compatibility for sessions created before cache timestamp key existed.
                GM_GetMasterConfigJson = cachedJson
                Exit Function
            End If
        End If
    End If

    On Error Resume Next
    Set http = Server.CreateObject("MSXML2.ServerXMLHTTP.6.0")
    If Err.Number <> 0 Then
        Err.Clear
        Set http = Server.CreateObject("MSXML2.ServerXMLHTTP")
    End If

    If Err.Number <> 0 Or (http Is Nothing) Then
        Err.Clear
        On Error GoTo 0
        GM_GetMasterConfigJson = ""
        Exit Function
    End If

    http.Open "GET", configUrl, False
    http.setTimeouts fetchTimeoutMs, fetchTimeoutMs, fetchTimeoutMs, fetchTimeoutMs
    http.setRequestHeader "Cache-Control", "no-cache"
    http.Send

    If Err.Number <> 0 Then
        Err.Clear
        Set http = Nothing
        On Error GoTo 0
        GM_GetMasterConfigJson = ""
        Exit Function
    End If

    If CLng(http.Status) <> 200 Then
        Set http = Nothing
        On Error GoTo 0
        GM_GetMasterConfigJson = ""
        Exit Function
    End If

    jsonText = CStr(http.responseText)
    Set http = Nothing
    On Error GoTo 0

    If Not GM_IsValidMasterConfigJson(jsonText) Then
        GM_GetMasterConfigJson = ""
        Exit Function
    End If

    If cacheEnabled Then
        Session("GM_MASTER_CONFIG_JSON") = jsonText
        Session("GM_MASTER_CONFIG_JSON_TS") = CStr(Now())
    End If
    GM_GetMasterConfigJson = jsonText
End Function


Function GM_MasterCfgGetString(cfgJsonText, keyName, defaultText)
    Dim v
    v = Trim(CStr(GM_JsonGetString(cfgJsonText, keyName)))
    If Len(v) = 0 Then v = Trim(CStr(defaultText))
    GM_MasterCfgGetString = v
End Function


Function GM_MasterCfgGetCsvInt(cfgJsonText, keyName, defaultCsv)
    Dim csvVal
    csvVal = CsvNormalizeIntList(GM_JsonGetIntArrayCsv(cfgJsonText, keyName, ""))
    If Len(csvVal) = 0 Then csvVal = CsvNormalizeIntList(GM_JsonGetString(cfgJsonText, keyName))
    If Len(csvVal) = 0 Then csvVal = CsvNormalizeIntList(defaultCsv)
    GM_MasterCfgGetCsvInt = csvVal
End Function


Function GM_MasterCfgGetInt(cfgJsonText, keyName, defaultInt)
    Dim rawText
    rawText = Trim(CStr(GM_JsonGetNumberText(cfgJsonText, keyName, CStr(defaultInt))))
    If IsNumeric(rawText) Then
        GM_MasterCfgGetInt = CLng(rawText)
    Else
        GM_MasterCfgGetInt = CLng(defaultInt)
    End If
End Function


Function GM_MasterCfgGetBool(cfgJsonText, keyName, defaultBool)
    Dim rawText
    rawText = Trim(CStr(GM_JsonGetBool01(cfgJsonText, keyName, "")))
    If rawText = "1" Then
        GM_MasterCfgGetBool = True
    ElseIf rawText = "0" Then
        GM_MasterCfgGetBool = False
    Else
        GM_MasterCfgGetBool = CBool(defaultBool)
    End If
End Function


Function GM_MasterCfgGetIsoDate(cfgJsonText, keyName, defaultIsoDate)
    Dim rawText, fallbackDate
    rawText = Trim(CStr(GM_JsonGetString(cfgJsonText, keyName)))
    fallbackDate = ParseIsoDateOrDefault(CStr(defaultIsoDate), Date())

    If Len(rawText) = 0 Then
        GM_MasterCfgGetIsoDate = ToIsoDate(fallbackDate)
        Exit Function
    End If

    GM_MasterCfgGetIsoDate = ToIsoDate(ParseIsoDateOrDefault(rawText, fallbackDate))
End Function

Function GetReportSortOrderByID(reportId)
    Dim n
    n = ToLongOrZero(GetReportConfigValueByID(reportId, "ReportDisplayOrder"))
    If n <= 0 Then n = ToLongOrZero(reportId)
    GetReportSortOrderByID = n
End Function

Function GM_NormalizeRemotePath(v)
    Dim t
    t = Trim(CStr(v))
    If Len(t) = 0 Then
        GM_NormalizeRemotePath = ""
        Exit Function
    End If

    Select Case UCase(t)
        Case "NULL", "N/A", "-"
            GM_NormalizeRemotePath = ""
        Case Else
            GM_NormalizeRemotePath = t
    End Select
End Function

Function GetReportHasKPIsByID(reportId)
    GetReportHasKPIsByID = (ToLongOrZero(GetReportConfigValueByID(reportId, "ReportHasKPIs")) > 0)
End Function

Function GetReportSqlUrlByID(reportId)
    GetReportSqlUrlByID = GM_NormalizeRemotePath(GetReportConfigValueByID(reportId, "ReportDataSqlFile"))
End Function

Function GetReportFiltersSqlUrlByID(reportId)
    GetReportFiltersSqlUrlByID = GM_NormalizeRemotePath(GetReportConfigValueByID(reportId, "ReportFiltersSqlFile"))
End Function

Function GetReportKPIsSqlUrlByID(reportId)
    GetReportKPIsSqlUrlByID = GM_NormalizeRemotePath(GetReportConfigValueByID(reportId, "ReportKPIsSqlFile"))
End Function

Function GetReportGraphSqlUrlByID(reportId)
    GetReportGraphSqlUrlByID = GM_NormalizeRemotePath(GetReportConfigValueByID(reportId, "ReportGraphSqlFile"))
End Function

Function BuildReportOptionsHTML(allowedReportIDs)
    Dim reportIDs, parts, used(), i, rid, outHtml
    Dim pickIndex, pickOrder, pickRID, thisOrder, thisRID
    Dim reportLabel, selectedRID

    reportIDs = GetEngineReportIDsCSV()
    outHtml = ""
    If Len(reportIDs) > 0 Then
        parts = Split(reportIDs, ",")
        ReDim used(UBound(parts))

        For i = 0 To UBound(parts)
            used(i) = False
        Next

        Do
            pickIndex = -1
            pickOrder = 2147483647
            pickRID = 2147483647

            For i = 0 To UBound(parts)
                If Not used(i) Then
                    thisRID = ToLongOrZero(parts(i))
                    thisOrder = GetReportSortOrderByID(thisRID)
                    If thisOrder < pickOrder Or (thisOrder = pickOrder And thisRID < pickRID) Then
                        pickIndex = i
                        pickOrder = thisOrder
                        pickRID = thisRID
                    End If
                End If
            Next

            If pickIndex = -1 Then Exit Do
            used(pickIndex) = True
            selectedRID = CStr(ToLongOrZero(parts(pickIndex)))

            If Len(selectedRID) > 0 Then
                If allowedReportIDs = "ALL" Or CsvContainsInt(allowedReportIDs, selectedRID) Then
                    reportLabel = GetReportNameByID(selectedRID)
                    If Len(Trim(reportLabel)) = 0 Then reportLabel = GetReportTitleByID(selectedRID)
                    outHtml = outHtml & "<option value=""" & Server.HTMLEncode(CStr(ToLongOrZero(selectedRID))) & """>" & Server.HTMLEncode(reportLabel) & "</option>"
                End If
            End If
        Loop
    End If
    If Len(Trim(outHtml)) = 0 Then outHtml = "<option value=""0"">" & Server.HTMLEncode(CStr(MsgStdReportUnavailable)) & "</option>"
    BuildReportOptionsHTML = outHtml
End Function

Sub ResolveReportMetadata()
    ReportID = ToLongOrZero(sSelectedReportID)
    If ReportID <= 0 Then ReportID = 0
    ReportParentCode = CStr(GetReportConfigValueByID(ReportID, "ReportParentCode"))
    ReportParentName = CStr(GetReportConfigValueByID(ReportID, "ReportParentName"))
    ReportDisplayOrder = ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportDisplayOrder"))
    ReportName = GetReportNameByID(ReportID)
    ReportTitle = GetReportTitleByID(ReportID)
    ReportAllowedProfileIDs = GetReportAllowedProfileIDsByID(ReportID)
    ReportVersion = CStr(GetReportConfigValueByID(ReportID, "ReportVersion"))
    ReportIsActive = GetReportIsActiveByID(ReportID)
    ReportNeedClinics = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportNeedClinics")) > 0)
    ReportNeedDates = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportNeedDates")) > 0)
    ReportHasFilters = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportHasFilters")) > 0)
    ReportFiltersSqlFile = CStr(GetReportConfigValueByID(ReportID, "ReportFiltersSqlFile"))
    ReportFiltersDefaultValues = CStr(GetReportConfigValueByID(ReportID, "ReportFiltersDefaultValues"))
    ReportDataHorizontalScroll = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportDataHorizontalScroll")) > 0)
    ReportDataSumPerRow = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportDataSumPerRow")) > 0)
    ReportDataSumPerColumn = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportDataSumPerColumn")) > 0)
    ReportDataSqlFile = CStr(GetReportConfigValueByID(ReportID, "ReportDataSqlFile"))
    ReportHasKPIs = GetReportHasKPIsByID(ReportID)
    ReportKPIsSqlFile = CStr(GetReportConfigValueByID(ReportID, "ReportKPIsSqlFile"))
    ReportHasGraph = (ToLongOrZero(GetReportConfigValueByID(ReportID, "ReportHasGraph")) > 0)
    ReportGraphConfig = CStr(GetReportConfigValueByID(ReportID, "ReportGraphConfig"))
    ReportGraphSqlFile = CStr(GetReportConfigValueByID(ReportID, "ReportGraphSqlFile"))
End Sub

'------------------------------------------------------------------------------
' SCOPE COMPUTATION - DATE CONTRACTS
'------------------------------------------------------------------------------
' Date range normalization and scope contract validation helpers.
Function ToIsoDate(d)
    ToIsoDate = Year(d) & "-" & Right("0" & Month(d), 2) & "-" & Right("0" & Day(d), 2)
End Function


Function ToIsoDateTime(d)
    ToIsoDateTime = Year(d) & "-" & Right("0" & Month(d), 2) & "-" & Right("0" & Day(d), 2) & " " & _
                    Right("0" & Hour(d), 2) & ":" & Right("0" & Minute(d), 2) & ":" & Right("0" & Second(d), 2)
End Function


Function ParseIsoDateOrDefault(v, fallbackDate)
    Dim t
    t = Trim(CStr(v))

    If Len(t) = 10 And Mid(t, 5, 1) = "-" And Mid(t, 8, 1) = "-" Then
        If IsNumeric(Left(t, 4)) And IsNumeric(Mid(t, 6, 2)) And IsNumeric(Right(t, 2)) Then
            On Error Resume Next
            ParseIsoDateOrDefault = DateSerial(CLng(Left(t, 4)), CLng(Mid(t, 6, 2)), CLng(Right(t, 2)))
            If Err.Number = 0 Then
                On Error GoTo 0
                Exit Function
            End If
            Err.Clear
            On Error GoTo 0
        End If
    End If

    ParseIsoDateOrDefault = fallbackDate
End Function


Function ValidateIsoDateString(v, fallbackDate)
    Dim d
    d = ParseIsoDateOrDefault(v, fallbackDate)
    ValidateIsoDateString = ToIsoDate(d)
End Function

Function FirstDayOfMonth(d)
    FirstDayOfMonth = DateSerial(Year(d), Month(d), 1)
End Function


Function LastDayOfMonth(d)
    LastDayOfMonth = DateSerial(Year(d), Month(d) + 1, 0)
End Function


Function NormalizeIsoDateInRange(v, fallbackDate, minDate, maxDate)
    Dim d
    d = ParseIsoDateOrDefault(v, fallbackDate)
    If d < minDate Then d = minDate
    If d > maxDate Then d = maxDate
    NormalizeIsoDateInRange = ToIsoDate(d)
End Function


'------------------------------------------------------------------------------
' SCOPE COMPUTATION - CLINIC CONTRACTS
'------------------------------------------------------------------------------
' Clinic contract helpers for permitted clinic scope enforcement.
Function Gm_SqlSafeIntList(v)
    Dim s, parts, i, token, outList
    s = Trim(CStr(v))
    outList = ""

    If Len(s) = 0 Then
        Gm_SqlSafeIntList = "0"
        Exit Function
    End If

    parts = Split(s, ",")
    For i = 0 To UBound(parts)
        token = Trim(parts(i))
        If Len(token) > 0 Then
            If Not IsNumeric(token) Then
                Gm_SqlSafeIntList = "0"
                Exit Function
            End If
            If Len(outList) > 0 Then outList = outList & ","
            outList = outList & CStr(CLng(token))
        End If
    Next

    If Len(outList) = 0 Then outList = "0"
    Gm_SqlSafeIntList = outList
End Function

'------------------------------------------------------------------------------
' ENGINE ORCHESTRATION
'------------------------------------------------------------------------------
' Builds execution scope dictionaries and coordinates runtime call contracts.
Function NewScopeDictionary()
    Set NewScopeDictionary = Server.CreateObject("Scripting.Dictionary")
End Function


Function BuildScopeBase()
    Dim d
    Set d = NewScopeDictionary()
    d.Add "ReportID", CStr(ToLongOrZero(sSelectedReportID))
    d.Add "ClinicIDs", CStr(CtxClinicIDs)
    d.Add "StartDate", ValidateIsoDateString(sStartDate, Date())
    d.Add "EndDate", ValidateIsoDateString(sEndDate, Date())
    d.Add "UserID", CStr(ToLongOrZero(EffectiveUserID))
    d.Add "Filter1", Null
    d.Add "Filter2", Null
    d.Add "Filter3", Null
    d.Add "Filter1IDs", ""
    d.Add "Filter2IDs", ""
    d.Add "Filter3IDs", ""

    If ReportHasFilters Then
        If Len(sReqFilter1IDs) > 0 Then
            d("Filter1") = ToLongOrZero(CsvFirst(sReqFilter1IDs, "0"))
            d("Filter1IDs") = sReqFilter1IDs
        End If
        If Len(sReqFilter2IDs) > 0 Then
            d("Filter2") = ToLongOrZero(CsvFirst(sReqFilter2IDs, "0"))
            d("Filter2IDs") = sReqFilter2IDs
        End If
        If Len(sReqFilter3IDs) > 0 Then
            d("Filter3") = ToLongOrZero(CsvFirst(sReqFilter3IDs, "0"))
            d("Filter3IDs") = sReqFilter3IDs
        End If
    End If

    Set BuildScopeBase = d
End Function

Function BuildScopeFull()
    Dim d
    Set d = BuildScopeBase()
    If ReportHasFilters Then
        If Len(sFilter1IDs) > 0 Then
            d("Filter1") = ToLongOrZero(CsvFirst(sFilter1IDs, sFilter1ID))
            d("Filter1IDs") = sFilter1IDs
        Else
            d("Filter1") = Null
            d("Filter1IDs") = ""
        End If

        If Len(sFilter2IDs) > 0 Then
            d("Filter2") = ToLongOrZero(CsvFirst(sFilter2IDs, sFilter2ID))
            d("Filter2IDs") = sFilter2IDs
        Else
            d("Filter2") = Null
            d("Filter2IDs") = ""
        End If

        If Len(sFilter3IDs) > 0 Then
            d("Filter3") = ToLongOrZero(CsvFirst(sFilter3IDs, sFilter3ID))
            d("Filter3IDs") = sFilter3IDs
        Else
            d("Filter3") = Null
            d("Filter3IDs") = ""
        End If
    Else
        d("Filter1") = Null
        d("Filter2") = Null
        d("Filter3") = Null
        d("Filter1IDs") = ""
        d("Filter2IDs") = ""
        d("Filter3IDs") = ""
    End If
    Set BuildScopeFull = d
End Function

'------------------------------------------------------------------------------
' SQL EXECUTION
'------------------------------------------------------------------------------
' Executes governed read-only datasets using the resolved Engine scope.

Function ExecuteFilterSQL(scopeDict)
    Dim sTmp, st, er, filtersUrl, naText
    st = "" : er = ""
    naText = Replace(CStr(MsgStdNA), "'", "''")
    If Len(Trim(naText)) = 0 Then naText = "N/A"
    filtersUrl = GetReportFiltersSqlUrlByID(ReportID)
    If Len(filtersUrl) = 0 Then
        sTmp = "SELECT 1 AS FilterLevel, 'Filtro 1' AS FilterLevelTitle, '' AS FilterIDs, '" & naText & "' AS FilterName LIMIT 0;"
    Else
        sTmp = GM_BuildExecutableSqlByUrl(filtersUrl, scopeDict, st, er)
    End If
    Set ExecuteFilterSQL = Gm_OpenRs(sTmp)
End Function

'------------------------------------------------------------------------------
' RENDER LAYER
'------------------------------------------------------------------------------
' Transforms SQL resultsets into filter controls and render-ready payloads.
Sub RenderSubfilters(filterRs)
    Dim req1, req2, req3
    Dim rowSep, colSep
    Dim raw1, raw2, raw3
    Dim first1IDs, first2IDs, first3IDs
    Dim first1Name, first2Name, first3Name
    Dim selected1Found, selected2Found, selected3Found
    Dim rowLevel, rowTitle, rowIDs, rowName
    Dim rows, cols, i, rowText, optIDs, optName, selectedAttr

    sFilter1ID = "0" : sFilter1IDs = "" : sFilter1Title = "Filtro 1" : sFilter1Name = "-" : sFilter1OptionsHTML = ""
    sFilter2ID = "0" : sFilter2IDs = "" : sFilter2Title = "Filtro 2" : sFilter2Name = "-" : sFilter2OptionsHTML = ""
    sFilter3ID = "0" : sFilter3IDs = "" : sFilter3Title = "Filtro 3" : sFilter3Name = "-" : sFilter3OptionsHTML = ""

    req1 = CsvNormalizeIntList(sReqFilter1IDs)
    req2 = CsvNormalizeIntList(sReqFilter2IDs)
    req3 = CsvNormalizeIntList(sReqFilter3IDs)

    rowSep = Chr(30)
    colSep = Chr(31)
    raw1 = "" : raw2 = "" : raw3 = ""
    first1IDs = "" : first1Name = ""
    first2IDs = "" : first2Name = ""
    first3IDs = "" : first3Name = ""
    selected1Found = False : selected2Found = False : selected3Found = False

    If filterRs Is Nothing Then Exit Sub
    If filterRs.State <> 1 Then Exit Sub
    If filterRs.EOF Then Exit Sub

    Do While Not filterRs.EOF
        rowLevel = ToLongOrZero(GM_RecordsetFieldTextOrBlank(filterRs, "FilterLevel"))
        rowTitle = GM_RecordsetFieldTextOrBlank(filterRs, "FilterLevelTitle")
        rowIDs = CsvNormalizeIntList(GM_RecordsetFieldTextOrBlank(filterRs, "FilterIDs"))
        rowName = GM_RecordsetFieldTextOrBlank(filterRs, "FilterName")

        If Len(rowName) = 0 Then rowName = "-"

        If (rowLevel >= 1 And rowLevel <= 3) And Len(rowIDs) > 0 Then
            Select Case rowLevel
                Case 1
                    If Len(rowTitle) > 0 Then sFilter1Title = rowTitle
                    If Len(first1IDs) = 0 Then
                        first1IDs = rowIDs
                        first1Name = rowName
                    End If
                    raw1 = raw1 & rowSep & rowIDs & colSep & rowName
                    If Len(req1) > 0 And rowIDs = req1 Then selected1Found = True
                Case 2
                    If Len(rowTitle) > 0 Then sFilter2Title = rowTitle
                    If Len(first2IDs) = 0 Then
                        first2IDs = rowIDs
                        first2Name = rowName
                    End If
                    raw2 = raw2 & rowSep & rowIDs & colSep & rowName
                    If Len(req2) > 0 And rowIDs = req2 Then selected2Found = True
                Case 3
                    If Len(rowTitle) > 0 Then sFilter3Title = rowTitle
                    If Len(first3IDs) = 0 Then
                        first3IDs = rowIDs
                        first3Name = rowName
                    End If
                    raw3 = raw3 & rowSep & rowIDs & colSep & rowName
                    If Len(req3) > 0 And rowIDs = req3 Then selected3Found = True
            End Select
        End If

        filterRs.MoveNext
    Loop

    If Len(req1) > 0 And selected1Found Then
        sFilter1IDs = req1
    Else
        sFilter1IDs = first1IDs
    End If

    If Len(req2) > 0 And selected2Found Then
        sFilter2IDs = req2
    Else
        sFilter2IDs = first2IDs
    End If

    If Len(req3) > 0 And selected3Found Then
        sFilter3IDs = req3
    Else
        sFilter3IDs = first3IDs
    End If

    If Len(raw1) > 0 Then
        rows = Split(Mid(raw1, 2), rowSep)
        For i = 0 To UBound(rows)
            rowText = CStr(rows(i))
            If Len(rowText) > 0 Then
                cols = Split(rowText, colSep)
                optIDs = CStr(cols(0))
                optName = "-"
                If UBound(cols) >= 1 Then optName = CStr(cols(1))
                selectedAttr = ""
                If optIDs = sFilter1IDs Then
                    selectedAttr = " selected"
                    sFilter1Name = optName
                End If
                sFilter1OptionsHTML = sFilter1OptionsHTML & "<option value=""" & Server.HTMLEncode(optIDs) & """" & selectedAttr & ">" & Server.HTMLEncode(optName) & "</option>"
            End If
        Next
        If Len(sFilter1Name) = 0 Or sFilter1Name = "-" Then sFilter1Name = first1Name
    End If

    If Len(raw2) > 0 Then
        rows = Split(Mid(raw2, 2), rowSep)
        For i = 0 To UBound(rows)
            rowText = CStr(rows(i))
            If Len(rowText) > 0 Then
                cols = Split(rowText, colSep)
                optIDs = CStr(cols(0))
                optName = "-"
                If UBound(cols) >= 1 Then optName = CStr(cols(1))
                selectedAttr = ""
                If optIDs = sFilter2IDs Then
                    selectedAttr = " selected"
                    sFilter2Name = optName
                End If
                sFilter2OptionsHTML = sFilter2OptionsHTML & "<option value=""" & Server.HTMLEncode(optIDs) & """" & selectedAttr & ">" & Server.HTMLEncode(optName) & "</option>"
            End If
        Next
        If Len(sFilter2Name) = 0 Or sFilter2Name = "-" Then sFilter2Name = first2Name
    End If

    If Len(raw3) > 0 Then
        rows = Split(Mid(raw3, 2), rowSep)
        For i = 0 To UBound(rows)
            rowText = CStr(rows(i))
            If Len(rowText) > 0 Then
                cols = Split(rowText, colSep)
                optIDs = CStr(cols(0))
                optName = "-"
                If UBound(cols) >= 1 Then optName = CStr(cols(1))
                selectedAttr = ""
                If optIDs = sFilter3IDs Then
                    selectedAttr = " selected"
                    sFilter3Name = optName
                End If
                sFilter3OptionsHTML = sFilter3OptionsHTML & "<option value=""" & Server.HTMLEncode(optIDs) & """" & selectedAttr & ">" & Server.HTMLEncode(optName) & "</option>"
            End If
        Next
        If Len(sFilter3Name) = 0 Or sFilter3Name = "-" Then sFilter3Name = first3Name
    End If

    sFilter1ID = CsvFirst(sFilter1IDs, "0")
    sFilter2ID = CsvFirst(sFilter2IDs, "0")
    sFilter3ID = CsvFirst(sFilter3IDs, "0")
End Sub

Function ExecuteKPIsSQL(scopeDict)
    Dim sTmp, st, er, kpisUrl, naText
    st = "" : er = ""
    naText = Replace(CStr(MsgStdNA), "'", "''")
    If Len(Trim(naText)) = 0 Then naText = "N/A"
    kpisUrl = GetReportKPIsSqlUrlByID(ReportID)
    If Len(kpisUrl) = 0 Then
        sTmp = "SELECT 0 AS KPIID, '" & naText & "' AS KPIName, '-' AS KPIValue, FALSE AS KPIScheme;"
    Else
        sTmp = GM_BuildExecutableSqlByUrl(kpisUrl, scopeDict, st, er)
    End If
    Set ExecuteKPIsSQL = Gm_OpenRs(sTmp)
End Function

Function ExecuteGraphSQL(scopeDict)
    Dim sTmp, st, er, graphUrl, naText
    st = "" : er = ""
    naText = Replace(CStr(MsgStdNA), "'", "''")
    If Len(Trim(naText)) = 0 Then naText = "N/A"
    graphUrl = GetReportGraphSqlUrlByID(ReportID)
    If Len(graphUrl) = 0 Then
        sTmp = "SELECT '" & naText & "' AS GraphLabel, 0 AS GraphValue LIMIT 0;"
    Else
        sTmp = GM_BuildExecutableSqlByUrl(graphUrl, scopeDict, st, er)
    End If
    Set ExecuteGraphSQL = Gm_OpenRs(sTmp)
End Function

Function ExecuteMainSQL(scopeDict)
    Dim sTmp, st, er, sqlTimeoutSec
    Dim rsTmp
    st = "" : er = ""
    sTmp = GM_BuildExecutableSql(ReportID, scopeDict, st, er)
    Set ExecuteMainSQL = Nothing

    Set rsTmp = Server.CreateObject("ADODB.Recordset")
    On Error Resume Next
    sqlTimeoutSec = ToLongOrZero(MasterSqlTimeoutSeconds)
    If sqlTimeoutSec > 0 Then
        If Not objConnection Is Nothing Then objConnection.CommandTimeout = sqlTimeoutSec
    End If
    Err.Clear
    ' Force client-side static cursor so RecordCount is available for logging.
    rsTmp.CursorLocation = 3
    rsTmp.Open sTmp, objConnection, 3, 1, 1
    If Err.Number = 0 Then
        Set ExecuteMainSQL = rsTmp
    Else
        Err.Clear
        If Not rsTmp Is Nothing Then
            If rsTmp.State = 1 Then rsTmp.Close
            Set rsTmp = Nothing
        End If
        Set ExecuteMainSQL = Gm_OpenRs(sTmp)
    End If
    On Error GoTo 0
End Function

Function GM_NvlText(v)
    On Error Resume Next
    If IsNull(v) Then
        GM_NvlText = ""
        On Error GoTo 0
        Exit Function
    End If
    GM_NvlText = CStr(v)
    If Err.Number <> 0 Then
        GM_NvlText = ""
        Err.Clear
    End If
    On Error GoTo 0
End Function

Function GM_RecordsetToJsonArray(rsObj)
    Dim outText, i, fieldName, fieldValue
    Dim hasRows

    outText = "["
    hasRows = False

    If rsObj Is Nothing Then
        GM_RecordsetToJsonArray = "[]"
        Exit Function
    End If

    On Error Resume Next
    If rsObj.State <> 1 Then
        Err.Clear
        On Error GoTo 0
        GM_RecordsetToJsonArray = "[]"
        Exit Function
    End If

    If rsObj.EOF Then
        On Error GoTo 0
        GM_RecordsetToJsonArray = "[]"
        Exit Function
    End If

    Do While Not rsObj.EOF
        If hasRows Then outText = outText & ","
        hasRows = True
        outText = outText & "{"

        For i = 0 To rsObj.Fields.Count - 1
            If i > 0 Then outText = outText & ","
            fieldName = CStr(rsObj.Fields(i).Name)
            outText = outText & """" & GmJsonEscape(fieldName) & """:"

            fieldValue = rsObj.Fields(i).Value
            If IsNull(fieldValue) Then
                outText = outText & "null"
            ElseIf VarType(fieldValue) = 11 Then
                If CBool(fieldValue) Then
                    outText = outText & "true"
                Else
                    outText = outText & "false"
                End If
            Else
                outText = outText & """" & GmJsonEscape(CStr(fieldValue)) & """"
            End If
        Next

        outText = outText & "}"
        rsObj.MoveNext
        If Err.Number <> 0 Then
            Err.Clear
            Exit Do
        End If
    Loop

    On Error GoTo 0
    outText = outText & "]"
    GM_RecordsetToJsonArray = outText
End Function

Function GM_GetDefaultGraphConfigJson()
    GM_GetDefaultGraphConfigJson = "{""chartType"":""bar"",""datasetLabel"":""Serie 1"",""mapping"":{""label"":""GraphLabel"",""value"":""GraphValue"",""series"":""""},""options"":{""legend"":true,""stacked"":false}}"
End Function

Function GM_ResolveGraphConfigJson(configValue)
    Dim cfgRef, cfgText, firstChar
    cfgRef = Trim(CStr(configValue))
    cfgText = ""

    If Len(cfgRef) = 0 Then
        GM_ResolveGraphConfigJson = GM_GetDefaultGraphConfigJson()
        Exit Function
    End If

    firstChar = Left(cfgRef, 1)
    If firstChar = "{" Or firstChar = "[" Then
        cfgText = cfgRef
    Else
        cfgText = Trim(CStr(GM_LoadRemoteSql(cfgRef)))
    End If

    cfgText = Trim(CStr(cfgText))
    If Len(cfgText) = 0 Then
        cfgText = GM_GetDefaultGraphConfigJson()
    Else
        firstChar = Left(cfgText, 1)
        If firstChar <> "{" And firstChar <> "[" Then
            cfgText = GM_GetDefaultGraphConfigJson()
        End If
    End If

    GM_ResolveGraphConfigJson = cfgText
End Function

Function GM_ElapsedMsFrom(startTimer)
    Dim nowTimer, elapsedSec
    GM_ElapsedMsFrom = 0

    On Error Resume Next
    nowTimer = CDbl(Timer())
    elapsedSec = nowTimer - CDbl(startTimer)
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If

    If elapsedSec < 0 Then elapsedSec = elapsedSec + 86400
    If elapsedSec < 0 Then elapsedSec = 0
    GM_ElapsedMsFrom = CLng(Round(elapsedSec * 1000))
    If GM_ElapsedMsFrom < 0 Then GM_ElapsedMsFrom = 0
    On Error GoTo 0
End Function

Function GM_GetRowsReturnedHint(rsObj)
    Dim n
    Dim rsClone
    GM_GetRowsReturnedHint = 0
    n = -1

    If rsObj Is Nothing Then Exit Function

    On Error Resume Next
    If rsObj.State <> 1 Then
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If

    If rsObj.EOF Then
        n = 0
    Else
        n = CLng(rsObj.RecordCount)
        If Err.Number <> 0 Then
            n = -1
            Err.Clear
        End If
    End If

    If n < 0 Then
        Set rsClone = Nothing
        Set rsClone = rsObj.Clone
        If Err.Number = 0 And Not rsClone Is Nothing Then
            n = 0
            If Not rsClone.EOF Then
                rsClone.MoveFirst
                Do While Not rsClone.EOF
                    n = n + 1
                    rsClone.MoveNext
                Loop
            End If
            If rsClone.State = 1 Then rsClone.Close
            Set rsClone = Nothing
        Else
            Err.Clear
            n = -1
        End If
    End If

    If n < 0 Then
        If rsObj.EOF Then
            n = 0
        Else
            n = -1
        End If
    End If

    GM_GetRowsReturnedHint = n
    On Error GoTo 0
End Function

Function GM_GetConfigSHA()
    Dim cfgText, i, code, sum1, sum2
    cfgText = ""

    On Error Resume Next
    cfgText = CStr(Session("GM_Config_JSON"))
    If Err.Number <> 0 Then
        cfgText = ""
        Err.Clear
    End If
    On Error GoTo 0

    If Len(cfgText) = 0 Then
        GM_GetConfigSHA = "NA"
        Exit Function
    End If

    sum1 = 1
    sum2 = 0
    For i = 1 To Len(cfgText)
        code = AscW(Mid(cfgText, i, 1))
        If code < 0 Then code = code + 65536
        sum1 = (sum1 + code) Mod 65521
        sum2 = (sum2 + sum1) Mod 65521
    Next

    GM_GetConfigSHA = CStr((CDbl(sum2) * 65536) + CDbl(sum1))
End Function

'------------------------------------------------------------------------------
' EXECUTION LOGGING
'------------------------------------------------------------------------------
' Registers execution telemetry and governance events for each runtime action.
Sub GM_LogEvent( _
    ExecutionType, ExecutionStatus, ExecutionErrorMessage, _
    RowsReturned, ExecutionDurationMs, _
    HostName, DatabaseName, UnmaskedIP, UserAgent, _
    EngineVersion, ConfigSHA, _
    AuthUserID, AuthUserName, AuthProfileID, AuthProfileDesc, AuthIsSuperAdmin, _
    EffectiveUserID, ReportID, ClinicIDs, StartDate, EndDate)

    On Error Resume Next

    Dim logUrl
    Dim uuid
    Dim timestamp
    Dim tsClient
    Dim statusCode
    Dim json
    Dim http
    Dim logTimeoutMs

    logUrl = Trim(CStr(PathLogEndpointValue))
    If Len(logUrl) = 0 Then logUrl = Trim(CStr(GM_LOG_WEBAPP_URL))
    If Len(logUrl) = 0 Then Exit Sub
    statusCode = 0
    logTimeoutMs = ToLongOrZero(MasterPerfHttpLogTimeoutMs)
    If logTimeoutMs <= 0 Then logTimeoutMs = 2000

    On Error Resume Next
    Session("LastLogError") = ""
    Err.Clear
    On Error GoTo 0

    uuid = ""
    On Error Resume Next
    uuid = Replace(CreateObject("Scriptlet.TypeLib").GUID, "{", "")
    uuid = Replace(uuid, "}", "")
    uuid = Replace(uuid, Chr(0), "")
    If Err.Number <> 0 Then
        uuid = CStr(Timer()) & "-" & CStr(Int(Rnd() * 1000000))
        Err.Clear
    End If
    On Error GoTo 0

    timestamp = Trim(CStr(CtxTimeStamp))
    If Len(timestamp) = 0 Then
        tsClient = ParseDateTimeFlexibleOrBlank(sCtxClientNow)
        If IsDate(tsClient) Then timestamp = ToIsoDateTime(CDate(tsClient))
    End If
    If Len(timestamp) = 0 Then
        timestamp = Year(Now()) & "-" & Right("0" & Month(Now()), 2) & "-" & _
                    Right("0" & Day(Now()), 2) & " " & _
                    Right("0" & Hour(Now()), 2) & ":" & _
                    Right("0" & Minute(Now()), 2) & ":" & _
                    Right("0" & Second(Now()), 2)
    End If

    json = "{""LogUUID"":""" & GmJsonEscape(GM_NvlText(uuid)) & """," & _
           """LogExecutionTimestamp"":""" & GmJsonEscape(GM_NvlText(timestamp)) & """," & _
           """LogExecutionType"":""" & GmJsonEscape(GM_NvlText(ExecutionType)) & """," & _
           """LogExecutionStatus"":""" & GmJsonEscape(GM_NvlText(ExecutionStatus)) & """," & _
           """LogExecutionErrorMessage"":""" & GmJsonEscape(GM_NvlText(ExecutionErrorMessage)) & """," & _
           """LogRowsReturned"":""" & GmJsonEscape(GM_NvlText(RowsReturned)) & """," & _
           """LogExecutionDurationMs"":""" & GmJsonEscape(GM_NvlText(ExecutionDurationMs)) & """," & _
           """LogHost"":""" & GmJsonEscape(GM_NvlText(HostName)) & """," & _
           """LogDatabase"":""" & GmJsonEscape(GM_NvlText(DatabaseName)) & """," & _
           """LogUnmaskedIP"":""" & GmJsonEscape(GM_NvlText(UnmaskedIP)) & """," & _
           """LogUserAgent"":""" & GmJsonEscape(GM_NvlText(UserAgent)) & """," & _
           """LogEngineVersion"":""" & GmJsonEscape(GM_NvlText(EngineVersion)) & """," & _
           """LogConfigSHA"":""" & GmJsonEscape(GM_NvlText(ConfigSHA)) & """," & _
           """LogAuthUserID"":""" & GmJsonEscape(GM_NvlText(AuthUserID)) & """," & _
           """LogAuthUserName"":""" & GmJsonEscape(GM_NvlText(AuthUserName)) & """," & _
           """LogAuthProfileID"":""" & GmJsonEscape(GM_NvlText(AuthProfileID)) & """," & _
           """LogAuthProfileDesc"":""" & GmJsonEscape(GM_NvlText(AuthProfileDesc)) & """," & _
           """LogAuthIsSuperAdmin"":""" & GmJsonEscape(GM_NvlText(AuthIsSuperAdmin)) & """," & _
           """LogEffectiveUserID"":""" & GmJsonEscape(GM_NvlText(EffectiveUserID)) & """," & _
           """LogReportID"":""" & GmJsonEscape(GM_NvlText(ReportID)) & """," & _
           """LogClinicIDs"":""" & GmJsonEscape(GM_NvlText(ClinicIDs)) & """," & _
           """LogStartDate"":""" & GmJsonEscape(GM_NvlText(StartDate)) & """," & _
           """LogEndDate"":""" & GmJsonEscape(GM_NvlText(EndDate)) & """}"

    ' LastLog must show the exact JSON sent to the logger endpoint.
    TxtWebhookPayload = json

    On Error Resume Next
    Set http = Server.CreateObject("MSXML2.ServerXMLHTTP.6.0")
    If Err.Number <> 0 Then
        Session("LastLogError") = "LOGGER_HTTP_CLIENT_INIT_ERROR: " & CStr(Err.Number) & " - " & CStr(Err.Description)
        Err.Clear
        Set http = Server.CreateObject("MSXML2.ServerXMLHTTP")
    End If

    If Err.Number <> 0 Or (http Is Nothing) Then
        Session("LastLogError") = "LOGGER_HTTP_CLIENT_INIT_ERROR: " & CStr(Err.Number) & " - " & CStr(Err.Description)
        Err.Clear
        On Error GoTo 0
        Exit Sub
    End If

    http.Open "POST", CStr(logUrl), False
    http.setTimeouts logTimeoutMs, logTimeoutMs, logTimeoutMs, logTimeoutMs
    http.setRequestHeader "Content-Type", "application/json"
    http.Send json

    If Err.Number <> 0 Then
        Session("LastLogError") = "LOGGER_SEND_ERROR: " & CStr(Err.Number) & " - " & CStr(Err.Description)
        Err.Clear
    Else
        statusCode = CLng(http.Status)
        If Err.Number <> 0 Then
            Session("LastLogError") = "LOGGER_STATUS_READ_ERROR: " & CStr(Err.Number) & " - " & CStr(Err.Description)
            Err.Clear
        ElseIf statusCode <> 200 Then
            Session("LastLogError") = "LOGGER_HTTP_STATUS: " & CStr(statusCode)
        Else
            Session("LastLogError") = ""
        End If
    End If

    Set http = Nothing
    On Error GoTo 0
End Sub

Sub Gm_LogReport(ActionType, scopeDict, Status, ErrorMessage)
    Dim scopeUserID, scopeClinicIDs, scopeStartDate, scopeEndDate
    Dim scopeFilter1, scopeFilter2, scopeFilter3
    Dim logExecStatus, logRowsReturned, logConfigSHA, logUnmaskedIP, logAuthIsSuperAdmin

    scopeUserID = CStr(EffectiveUserID)
    scopeClinicIDs = ""
    scopeStartDate = ""
    scopeEndDate = ""
    scopeFilter1 = ""
    scopeFilter2 = ""
    scopeFilter3 = ""

    On Error Resume Next
    If IsObject(scopeDict) Then
        If scopeDict.Exists("UserID") Then scopeUserID = CStr(scopeDict("UserID"))
        If scopeDict.Exists("ClinicIDs") Then scopeClinicIDs = CStr(scopeDict("ClinicIDs"))
        If scopeDict.Exists("StartDate") Then scopeStartDate = CStr(scopeDict("StartDate"))
        If scopeDict.Exists("EndDate") Then scopeEndDate = CStr(scopeDict("EndDate"))

        If scopeDict.Exists("Filter1") Then
            If Not IsNull(scopeDict("Filter1")) Then scopeFilter1 = CStr(scopeDict("Filter1"))
        End If
        If scopeDict.Exists("Filter2") Then
            If Not IsNull(scopeDict("Filter2")) Then scopeFilter2 = CStr(scopeDict("Filter2"))
        End If
        If scopeDict.Exists("Filter3") Then
            If Not IsNull(scopeDict("Filter3")) Then scopeFilter3 = CStr(scopeDict("Filter3"))
        End If
    End If
    Err.Clear
    On Error GoTo 0

    logExecStatus = UCase(Trim(CStr(Status)))
    If logExecStatus = "SUCCESS" Or logExecStatus = "OK" Then
        logExecStatus = "OK"
    Else
        logExecStatus = "ERROR"
    End If

    Select Case UCase(Trim(CStr(ActionType)))
        Case "RUN", "EXPORT"
            logRowsReturned = CStr(GM_GetRowsReturnedHint(rsData))
        Case Else
            logRowsReturned = "0"
    End Select

    logUnmaskedIP = FirstIPToken(GetServerVarText("REMOTE_ADDR"))
    If Len(Trim(logUnmaskedIP)) = 0 Then logUnmaskedIP = FirstIPToken(sSessionUip)
    If Len(Trim(logUnmaskedIP)) = 0 Then logUnmaskedIP = "-"

    If AuthIsSuperAdmin Then
        logAuthIsSuperAdmin = "1"
    Else
        logAuthIsSuperAdmin = "0"
    End If

    logConfigSHA = GM_GetConfigSHA()

    Call GM_LogEvent( _
        CStr(ActionType), _
        logExecStatus, _
        CStr(ErrorMessage), _
        logRowsReturned, _
        CStr(ToLongOrZero(ExecutionTimeMs)), _
        CStr(sFlowwwHost), _
        CStr(sCurrentDBName), _
        logUnmaskedIP, _
        GetServerVarText("HTTP_USER_AGENT"), _
        EngineVersionValue, _
        logConfigSHA, _
        CStr(AuthUserID), _
        CStr(AuthUserName), _
        CStr(AuthProfileID), _
        CStr(AuthProfileDesc), _
        logAuthIsSuperAdmin, _
        CStr(EffectiveUserID), _
        CStr(ToLongOrZero(ReportID)), _
        scopeClinicIDs, _
        scopeStartDate, _
        scopeEndDate _
    )
End Sub

Function CsvEscape(v)
    CsvEscape = """" & Replace(CStr(v), """", """""") & """"
End Function

Function GmSafeFileToken(v)
    Dim s, i, ch, code, outText
    s = Trim(CStr(v))
    outText = ""

    For i = 1 To Len(s)
        ch = Mid(s, i, 1)
        code = AscW(ch)
        If code < 0 Then code = code + 65536

        If (code >= 48 And code <= 57) Or (code >= 65 And code <= 90) Or (code >= 97 And code <= 122) Then
            outText = outText & ch
        ElseIf ch = "_" Or ch = "-" Then
            outText = outText & ch
        ElseIf ch = " " Then
            outText = outText & "_"
        End If
    Next

    Do While InStr(outText, "__") > 0
        outText = Replace(outText, "__", "_")
    Loop

    If Len(Trim(outText)) = 0 Then outText = "Reporte"
    GmSafeFileToken = outText
End Function


Sub ExportMain(mainRs)
    Dim i, lineOut, cellValue, fileName
    fileName = GmSafeFileToken(GmAsciiSafe(ReportTitle)) & "_" & Replace(ToIsoDate(Date()), "-", "")

    Response.Clear
    Response.Buffer = True
    Response.Charset = "windows-1252"
    Response.ContentType = "text/csv"
    Response.AddHeader "Content-Disposition", "attachment; filename=" & fileName & ".csv"

    If mainRs Is Nothing Then
        Response.Write CsvEscape("Mensaje") & vbCrLf & CsvEscape("Sin datos")
        Response.Flush
        Response.End
        Exit Sub
    End If

    If mainRs.State <> 1 Then
        Response.Write CsvEscape("Mensaje") & vbCrLf & CsvEscape("Sin datos")
        Response.Flush
        Response.End
        Exit Sub
    End If

    lineOut = ""
    For i = 0 To mainRs.Fields.Count - 1
        If i > 0 Then lineOut = lineOut & ","
        lineOut = lineOut & CsvEscape(mainRs.Fields(i).Name)
    Next
    Response.Write lineOut & vbCrLf

    If mainRs.EOF Then
        Response.Flush
        Response.End
        Exit Sub
    End If

    Do While Not mainRs.EOF
        lineOut = ""
        For i = 0 To mainRs.Fields.Count - 1
            If i > 0 Then lineOut = lineOut & ","
            If IsNull(mainRs.Fields(i).Value) Then
                cellValue = "-"
            Else
                cellValue = CStr(mainRs.Fields(i).Value)
            End If
            lineOut = lineOut & CsvEscape(cellValue)
        Next
        Response.Write lineOut & vbCrLf
        mainRs.MoveNext
    Loop

    Response.Flush
    Response.End
End Sub

Sub Gm_ExportReport()
    On Error Resume Next
    t0 = Timer
    Set ScopeFull = BuildScopeFull()
    Set rsData = ExecuteMainSQL(ScopeFull)
    t1 = Timer
    ExecutionTimeMs = GM_ElapsedMsFrom(RequestStartTimer)
    If Err.Number = 0 Then
        Call Gm_LogReport("EXPORT", ScopeFull, "SUCCESS", "")
        On Error GoTo 0
        Call ExportMain(rsData)
        Exit Sub
    Else
        Call Gm_LogReport("EXPORT", ScopeFull, "FAIL", Err.Description)
        Err.Clear
    End If
    On Error GoTo 0
End Sub


Sub Gm_ExecuteReport()
    Dim rsFilters
    On Error Resume Next
    t0 = Timer
    Set ScopeBase = BuildScopeBase()

    If ReportHasFilters Then
        Set rsFilters = ExecuteFilterSQL(ScopeBase)
        Call RenderSubfilters(rsFilters)
        If Not rsFilters Is Nothing Then
            If rsFilters.State = 1 Then rsFilters.Close
            Set rsFilters = Nothing
        End If
    Else
        sFilter1ID = "0" : sFilter1IDs = "" : sFilter1Title = "Filtro 1" : sFilter1Name = "-" : sFilter1OptionsHTML = ""
        sFilter2ID = "0" : sFilter2IDs = "" : sFilter2Title = "Filtro 2" : sFilter2Name = "-" : sFilter2OptionsHTML = ""
        sFilter3ID = "0" : sFilter3IDs = "" : sFilter3Title = "Filtro 3" : sFilter3Name = "-" : sFilter3OptionsHTML = ""
    End If

    Set ScopeFull = BuildScopeFull()
    If ReportHasKPIs Then
        Set rsKPIs = ExecuteKPIsSQL(ScopeFull)
    Else
        Set rsKPIs = Nothing
    End If
    If ReportHasGraph Then
        Set rsGraph = ExecuteGraphSQL(ScopeFull)
    Else
        Set rsGraph = Nothing
    End If
    Set rsData = ExecuteMainSQL(ScopeFull)
    t1 = Timer
    ExecutionTimeMs = GM_ElapsedMsFrom(RequestStartTimer)
    If Err.Number = 0 Then
        Call Gm_LogReport("RUN", ScopeFull, "SUCCESS", "")
    Else
        Call Gm_LogReport("RUN", ScopeFull, "FAIL", Err.Description)
        Err.Clear
    End If
    On Error GoTo 0
End Sub

'------------------------------------------------------------------------------
' RENDER LAYER - SUPPORT HELPERS
'------------------------------------------------------------------------------
' Shared rendering and payload helpers used by runtime orchestration flows.
Sub Gm_BuildTxtMetadatos()
    Dim ConnHost, ConnDB, ConnMySQL, ConnIPMasked, ConnTZ, ReportDisplayName
    ConnHost = sFlowwwHost
    ConnDB = sCurrentDBName
    ConnMySQL = sDbVersion
    ConnIPMasked = sMetaIPMasked
    ConnTZ = Trim(CStr(AuthSessionTZ))
    If Len(ConnTZ) = 0 Then ConnTZ = CStr(MsgStdNotAvailable)
    If Len(Trim(ConnTZ)) = 0 Then ConnTZ = "No disponible"
    ReportDisplayName = Trim(CStr(ReportTitle))
    If Len(ReportDisplayName) = 0 Then ReportDisplayName = Trim(CStr(ReportName))
    If Len(ReportDisplayName) = 0 Then ReportDisplayName = "-"

    TxtMetadatos = "Conexion:" & vbCrLf & _
                   "- Host: " & ConnHost & vbCrLf & _
                   "- Base de Datos: " & ConnDB & vbCrLf & _
                   "- MySQL: " & ConnMySQL & vbCrLf & _
                   "- IP: " & ConnIPMasked & vbCrLf & _
                   "- TimeZone: " & ConnTZ & vbCrLf & vbCrLf & _
                   "Autenticacion:" & vbCrLf & _
                   "- Sesion: " & AuthSessionID & " - " & AuthSessionStart & vbCrLf & _
                   "- Usuario: " & AuthUserID & " - " & AuthUserName & vbCrLf & _
                   "- Perfil: " & AuthProfileID & " - " & AuthProfileDesc & vbCrLf & _
                   "- Centro: " & AuthClinicID & " - " & AuthPermClinicsIDs & vbCrLf & vbCrLf & _
                   "Aplicacion:" & vbCrLf & _
                   "- Engine: " & EngineName & " - " & EngineConfigName & " (" & CStr(EngineReportCount) & ")" & vbCrLf & _
                   "- Environment: " & CStr(MasterEngineEnvironment) & vbCrLf

    If AuthIsSuperAdmin Then
        TxtMetadatos = TxtMetadatos & _
                       "- _Usuario: " & EffectiveUserID & " - " & CtxUserName & vbCrLf & _
                       "- _Perfil: " & EffectiveProfileID & " - " & EffectiveProfileDesc & vbCrLf
    End If

    TxtMetadatos = TxtMetadatos & _
                   "- Reporte: " & CStr(ReportID) & " - " & ReportDisplayName & " (" & EffectiveAllowedReportIDs & ")" & vbCrLf & _
                   "- Centro: " & CtxClinicIDs & " (" & EffectiveAllowedClinicIDs & ")" & vbCrLf & _
                   "- Rango: " & sStartDate & " - " & sEndDate & vbCrLf

    If ReportHasFilters Then
        TxtMetadatos = TxtMetadatos & _
                       "- Filtros: " & sFilter1IDs & " - " & sFilter2IDs & " - " & sFilter3IDs & vbCrLf
    End If

    TxtMetadatos = TxtMetadatos & _
                   "- CSS: " & CtxCSSpx & vbCrLf & _
                   "- Timestamp: " & CtxTimeStamp & vbCrLf
End Sub

Function GmAsciiSafe(v)
    Dim s, i, ch, code, outText
    s = CStr(v)
    outText = ""

    For i = 1 To Len(s)
        ch = Mid(s, i, 1)
        code = AscW(ch)
        If code < 0 Then code = code + 65536

        If (code >= 32 And code <= 126) Or code = 9 Or code = 10 Or code = 13 Then
            outText = outText & ch
        Else
            outText = outText & " "
        End If
    Next

    GmAsciiSafe = outText
End Function


Function GmJsonEscape(v)
    Dim s, i, ch, code, outText
    s = CStr(v)
    s = Replace(s, Chr(0), "")
    s = Replace(s, "\", "\\")
    s = Replace(s, """", "\" & """")
    s = Replace(s, vbCrLf, "\n")
    s = Replace(s, vbCr, "\n")
    s = Replace(s, vbLf, "\n")
    s = Replace(s, vbTab, "\t")

    outText = ""
    For i = 1 To Len(s)
        ch = Mid(s, i, 1)
        code = AscW(ch)
        If code < 0 Then code = code + 65536
        If code >= 32 Then outText = outText & ch
    Next

    GmJsonEscape = outText
End Function


Function BuildWaUrl(phone, message)
    BuildWaUrl = "https://wa.me/" & phone & "?text=" & Replace(Server.URLEncode(message), "+", "%20")
End Function


Function NormalizeCtxCssPx(v)
    Dim t, core, parts, w, h
    t = LCase(Trim(CStr(v)))
    t = Replace(t, " ", "")
    NormalizeCtxCssPx = ""
    If Len(t) = 0 Then Exit Function

    If Right(t, 2) = "px" Then
        core = Left(t, Len(t) - 2)
    Else
        core = t
    End If

    If InStr(1, core, "x", vbTextCompare) = 0 Then Exit Function
    parts = Split(core, "x")
    If UBound(parts) <> 1 Then Exit Function
    If Not IsNumeric(parts(0)) Or Not IsNumeric(parts(1)) Then Exit Function

    w = CLng(parts(0))
    h = CLng(parts(1))
    If w <= 0 Or h <= 0 Then Exit Function
    NormalizeCtxCssPx = CStr(w) & "x" & CStr(h) & "px"
End Function


Function GM_LoadRemoteSql(url)
    Dim http, result, statusCode, fetchTimeoutMs
    result = ""
    statusCode = 0
    fetchTimeoutMs = ToLongOrZero(MasterPerfHttpFetchTimeoutMs)
    If fetchTimeoutMs <= 0 Then fetchTimeoutMs = 5000

    On Error Resume Next
    Set http = Server.CreateObject("MSXML2.ServerXMLHTTP.6.0")
    If Err.Number <> 0 Then
        Err.Clear
        Set http = Server.CreateObject("MSXML2.ServerXMLHTTP")
    End If

    If Err.Number <> 0 Or (http Is Nothing) Then
        result = "-- ERROR loading SQL. Unable to initialize HTTP client."
        Err.Clear
        On Error GoTo 0
        GM_LoadRemoteSql = result
        Exit Function
    End If

    http.Open "GET", CStr(url), False
    http.setTimeouts fetchTimeoutMs, fetchTimeoutMs, fetchTimeoutMs, fetchTimeoutMs
    http.setRequestHeader "Cache-Control", "no-cache"
    http.Send

    If Err.Number <> 0 Then
        result = "-- ERROR loading SQL. " & Err.Description
        Err.Clear
    Else
        statusCode = CLng(http.Status)
        If statusCode = 200 Then
            result = CStr(http.responseText)
            If Len(Trim(result)) = 0 Then result = "-- ERROR loading SQL. Empty response body."
        Else
            result = "-- ERROR loading SQL. HTTP Status: " & CStr(statusCode)
        End If
    End If

    Set http = Nothing
    On Error GoTo 0

    GM_LoadRemoteSql = result
End Function


Function GM_StripSqlComments(s)
    Dim txt, startPos, endPos
    Dim lines, i, lineText, outText, prevBlank
    txt = CStr(s)

    Do
        startPos = InStr(1, txt, "/*", vbTextCompare)
        If startPos = 0 Then Exit Do

        endPos = InStr(startPos + 2, txt, "*/", vbTextCompare)
        If endPos = 0 Then
            txt = Left(txt, startPos - 1)
            Exit Do
        End If

        txt = Left(txt, startPos - 1) & Mid(txt, endPos + 2)
    Loop

    txt = Replace(txt, vbCrLf, vbLf)
    txt = Replace(txt, vbCr, vbLf)
    lines = Split(txt, vbLf)
    outText = ""
    prevBlank = False

    For i = 0 To UBound(lines)
        lineText = CStr(lines(i))
        If Len(Trim(lineText)) = 0 Then
            If Not prevBlank And Len(outText) > 0 Then outText = outText & vbLf
            prevBlank = True
        Else
            If Len(outText) > 0 And Right(outText, 1) <> vbLf Then outText = outText & vbLf
            outText = outText & lineText
            prevBlank = False
        End If
    Next

    outText = Trim(outText)
    outText = Replace(outText, vbLf, vbCrLf)
    GM_StripSqlComments = outText
End Function


Function GM_ReplaceAliasExpression(sqlText, aliasName, replacementExpr)
    Dim txt, re, pattern, replaceText
    txt = CStr(sqlText)
    replaceText = CStr(replacementExpr) & " AS " & CStr(aliasName)

    On Error Resume Next
    Set re = Server.CreateObject("VBScript.RegExp")
    If Err.Number <> 0 Then
        Err.Clear
        On Error GoTo 0
        GM_ReplaceAliasExpression = txt
        Exit Function
    End If
    On Error GoTo 0

    re.Global = True
    re.IgnoreCase = True
    re.MultiLine = True
    pattern = "^([ \t]*).+?[ \t]+AS[ \t]+" & CStr(aliasName) & "([ \t]*,?)"
    re.Pattern = pattern

    GM_ReplaceAliasExpression = re.Replace(txt, "$1" & replaceText & "$2")
    Set re = Nothing
End Function


Function GM_InjectScopeIntoSql(sSQL, scopeDict)

    Dim txt
    Dim vUserID, vClinicIDs, vStartDate, vEndDate
    Dim vFilter1, vFilter2, vFilter3
    Dim vFilter1IDs, vFilter2IDs, vFilter3IDs
    Dim vFilter1IDsExpr, vFilter2IDsExpr, vFilter3IDsExpr

    txt = CStr(sSQL)

    vUserID = CStr(ToLongOrZero(scopeDict("UserID")))
    vClinicIDs = CsvNormalizeIntList(scopeDict("ClinicIDs"))
    vStartDate = ValidateIsoDateString(scopeDict("StartDate"), Date())
    vEndDate = ValidateIsoDateString(scopeDict("EndDate"), Date())

    If IsNull(scopeDict("Filter1")) Then
        vFilter1 = "NULLIF(1,1)"
    Else
        vFilter1 = CStr(ToLongOrZero(scopeDict("Filter1")))
    End If

    If IsNull(scopeDict("Filter2")) Then
        vFilter2 = "NULLIF(1,1)"
    Else
        vFilter2 = CStr(ToLongOrZero(scopeDict("Filter2")))
    End If

    If IsNull(scopeDict("Filter3")) Then
        vFilter3 = "NULLIF(1,1)"
    Else
        vFilter3 = CStr(ToLongOrZero(scopeDict("Filter3")))
    End If

    vFilter1IDs = ""
    vFilter2IDs = ""
    vFilter3IDs = ""
    If IsObject(scopeDict) Then
        If scopeDict.Exists("Filter1IDs") Then vFilter1IDs = CsvNormalizeIntList(scopeDict("Filter1IDs"))
        If scopeDict.Exists("Filter2IDs") Then vFilter2IDs = CsvNormalizeIntList(scopeDict("Filter2IDs"))
        If scopeDict.Exists("Filter3IDs") Then vFilter3IDs = CsvNormalizeIntList(scopeDict("Filter3IDs"))
    End If

    If Len(vFilter1IDs) = 0 Then
        vFilter1IDsExpr = "NULLIF(1,1)"
    Else
        vFilter1IDsExpr = "'" & vFilter1IDs & "'"
    End If

    If Len(vFilter2IDs) = 0 Then
        vFilter2IDsExpr = "NULLIF(1,1)"
    Else
        vFilter2IDsExpr = "'" & vFilter2IDs & "'"
    End If

    If Len(vFilter3IDs) = 0 Then
        vFilter3IDsExpr = "NULLIF(1,1)"
    Else
        vFilter3IDsExpr = "'" & vFilter3IDs & "'"
    End If

    txt = Replace(txt, "NULL AS EngineUserID", vUserID & " AS EngineUserID")
    txt = Replace(txt, "0            AS EngineUserID", vUserID & " AS EngineUserID")
    txt = Replace(txt, "0 AS EngineUserID", vUserID & " AS EngineUserID")

    txt = Replace(txt, "''   AS EngineClinicIDs", "'" & vClinicIDs & "' AS EngineClinicIDs")
    txt = Replace(txt, "''           AS EngineClinicIDs", "'" & vClinicIDs & "' AS EngineClinicIDs")
    txt = Replace(txt, "'' AS EngineClinicIDs", "'" & vClinicIDs & "' AS EngineClinicIDs")

    txt = Replace(txt, "NULL AS EngineStartDate", "'" & vStartDate & "' AS EngineStartDate")
    txt = Replace(txt, "'2000-01-01' AS EngineStartDate", "'" & vStartDate & "' AS EngineStartDate")

    txt = Replace(txt, "NULL AS EngineEndDate", "'" & vEndDate & "' AS EngineEndDate")
    txt = Replace(txt, "'2000-01-07' AS EngineEndDate", "'" & vEndDate & "' AS EngineEndDate")

    txt = Replace(txt, "NULL AS EngineFilter1", vFilter1 & " AS EngineFilter1")
    txt = Replace(txt, "0            AS EngineFilter1", vFilter1 & " AS EngineFilter1")
    txt = Replace(txt, "0 AS EngineFilter1", vFilter1 & " AS EngineFilter1")

    txt = Replace(txt, "NULL AS EngineFilter2", vFilter2 & " AS EngineFilter2")
    txt = Replace(txt, "0            AS EngineFilter2", vFilter2 & " AS EngineFilter2")
    txt = Replace(txt, "0 AS EngineFilter2", vFilter2 & " AS EngineFilter2")

    txt = Replace(txt, "NULL AS EngineFilter3", vFilter3 & " AS EngineFilter3")
    txt = Replace(txt, "0            AS EngineFilter3", vFilter3 & " AS EngineFilter3")
    txt = Replace(txt, "0 AS EngineFilter3", vFilter3 & " AS EngineFilter3")

    txt = Replace(txt, "NULL AS EngineFilter1IDs", vFilter1IDsExpr & " AS EngineFilter1IDs")
    txt = Replace(txt, "'' AS EngineFilter1IDs", vFilter1IDsExpr & " AS EngineFilter1IDs")
    txt = Replace(txt, "NULL AS EngineFilter2IDs", vFilter2IDsExpr & " AS EngineFilter2IDs")
    txt = Replace(txt, "'' AS EngineFilter2IDs", vFilter2IDsExpr & " AS EngineFilter2IDs")
    txt = Replace(txt, "NULL AS EngineFilter3IDs", vFilter3IDsExpr & " AS EngineFilter3IDs")
    txt = Replace(txt, "'' AS EngineFilter3IDs", vFilter3IDsExpr & " AS EngineFilter3IDs")

    txt = Replace(txt, "NULL AS EngineFilter1CSV", vFilter1IDsExpr & " AS EngineFilter1CSV")
    txt = Replace(txt, "'' AS EngineFilter1CSV", vFilter1IDsExpr & " AS EngineFilter1CSV")
    txt = Replace(txt, "NULL AS EngineFilter2CSV", vFilter2IDsExpr & " AS EngineFilter2CSV")
    txt = Replace(txt, "'' AS EngineFilter2CSV", vFilter2IDsExpr & " AS EngineFilter2CSV")
    txt = Replace(txt, "NULL AS EngineFilter3CSV", vFilter3IDsExpr & " AS EngineFilter3CSV")
    txt = Replace(txt, "'' AS EngineFilter3CSV", vFilter3IDsExpr & " AS EngineFilter3CSV")

    txt = GM_ReplaceAliasExpression(txt, "EngineUserID", vUserID)
    txt = GM_ReplaceAliasExpression(txt, "EngineClinicIDs", "'" & vClinicIDs & "'")
    txt = GM_ReplaceAliasExpression(txt, "EngineStartDate", "'" & vStartDate & "'")
    txt = GM_ReplaceAliasExpression(txt, "EngineEndDate", "'" & vEndDate & "'")
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter1", vFilter1)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter2", vFilter2)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter3", vFilter3)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter1IDs", vFilter1IDsExpr)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter2IDs", vFilter2IDsExpr)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter3IDs", vFilter3IDsExpr)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter1CSV", vFilter1IDsExpr)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter2CSV", vFilter2IDsExpr)
    txt = GM_ReplaceAliasExpression(txt, "EngineFilter3CSV", vFilter3IDsExpr)

    GM_InjectScopeIntoSql = txt

End Function


Function GM_HasUnresolvedEnginePlaceholders(sqlText)
    Dim s, unresolvedPatterns, i
    s = LCase(CStr(sqlText))

    s = Replace(s, vbCrLf, " ")
    s = Replace(s, vbCr, " ")
    s = Replace(s, vbLf, " ")
    s = Replace(s, vbTab, " ")
    Do While InStr(s, "  ") > 0
        s = Replace(s, "  ", " ")
    Loop
    s = " " & s & " "

    unresolvedPatterns = Array( _
        " null as engineuserid", _
        " '' as engineclinicids", _
        " null as enginestartdate", _
        " null as engineenddate", _
        " null as enginefilter1", _
        " null as enginefilter2", _
        " null as enginefilter3", _
        " null as enginefilter1ids", _
        " null as enginefilter2ids", _
        " null as enginefilter3ids", _
        " null as enginefilter1csv", _
        " null as enginefilter2csv", _
        " null as enginefilter3csv", _
        " '' as enginefilter1ids", _
        " '' as enginefilter2ids", _
        " '' as enginefilter3ids", _
        " '' as enginefilter1csv", _
        " '' as enginefilter2csv", _
        " '' as enginefilter3csv", _
        " cast(null as signed) as engineuserid", _
        " cast(null as char) as engineclinicids", _
        " cast(null as date) as enginestartdate", _
        " cast(null as date) as engineenddate", _
        " cast(null as signed) as enginefilter1", _
        " cast(null as signed) as enginefilter2", _
        " cast(null as signed) as enginefilter3", _
        " cast(null as char) as enginefilter1ids", _
        " cast(null as char) as enginefilter2ids", _
        " cast(null as char) as enginefilter3ids", _
        " cast(null as char) as enginefilter1csv", _
        " cast(null as char) as enginefilter2csv", _
        " cast(null as char) as enginefilter3csv" _
    )

    For i = 0 To UBound(unresolvedPatterns)
        If InStr(1, s, unresolvedPatterns(i), vbTextCompare) > 0 Then
            GM_HasUnresolvedEnginePlaceholders = True
            Exit Function
        End If
    Next

    GM_HasUnresolvedEnginePlaceholders = False
End Function

Function GM_IsSafeSelect(sSQL)

    Dim s
    s = LCase(Trim(CStr(sSQL)))

    If Len(s) = 0 Then
        GM_IsSafeSelect = False
        Exit Function
    End If

    s = Replace(s, vbCrLf, " ")
    s = Replace(s, vbCr, " ")
    s = Replace(s, vbLf, " ")
    s = Replace(s, vbTab, " ")

    Do While InStr(s, "  ") > 0
        s = Replace(s, "  ", " ")
    Loop

    s = " " & s & " "

    If Left(Trim(s), 6) <> "select" Then
        GM_IsSafeSelect = False
        Exit Function
    End If

    If InStr(s, " from ") = 0 Then
        GM_IsSafeSelect = False
        Exit Function
    End If

    Dim forbidden
    forbidden = Array( _
        " drop ", _
        " delete ", _
        " update ", _
        " insert ", _
        " truncate ", _
        " alter ", _
        " create table ", _
        " create database ", _
        " replace ", _
        " exec ", _
        " call ", _
        " into outfile ", _
        " load data ", _
        " grant ", _
        " revoke ", _
        " shutdown ", _
        " lock tables " _
    )

    Dim i
    For i = 0 To UBound(forbidden)
        If InStr(s, forbidden(i)) > 0 Then
            GM_IsSafeSelect = False
            Exit Function
        End If
    Next

    GM_IsSafeSelect = True

End Function


Function GM_BuildExecutableSqlByUrl(sqlUrl, scopeDict, ByRef outStatus, ByRef outError)
    Dim url, rawSql, cleanSql, injectedSql
    Dim msgSqlNotSafe, msgSqlPlaceholders, msgSqlEmpty, msgStdNotAvailableLocal
    Dim isSafe, hasUnresolved

    outStatus = "FAIL"
    outError = ""
    msgSqlNotSafe = Replace(CStr(MsgStdSqlNotSafe), "'", "''")
    msgSqlPlaceholders = Replace(CStr(MsgStdSqlPlaceholdersUnresolved), "'", "''")
    msgSqlEmpty = Replace(CStr(MsgStdSqlEmptyAfterBuild), "'", "''")
    msgStdNotAvailableLocal = Replace(CStr(MsgStdNotAvailable), "'", "''")
    If Len(Trim(msgSqlNotSafe)) = 0 Then msgSqlNotSafe = "VALIDATION FAILED - NOT SAFE"
    If Len(Trim(msgSqlPlaceholders)) = 0 Then msgSqlPlaceholders = "VALIDATION FAILED - Engine* placeholders unresolved"
    If Len(Trim(msgSqlEmpty)) = 0 Then msgSqlEmpty = "SQL empty after build"
    If Len(Trim(msgStdNotAvailableLocal)) = 0 Then msgStdNotAvailableLocal = "SQL URL missing"

    url = GM_NormalizeRemotePath(sqlUrl)
    If Len(url) = 0 Then
        outError = "SQL_URL_MISSING"
        GM_BuildExecutableSqlByUrl = "SELECT '" & msgStdNotAvailableLocal & "' AS Mensaje;"
        Exit Function
    End If

    rawSql = GM_LoadRemoteSql(url)
    cleanSql = GM_StripSqlComments(rawSql)
    injectedSql = GM_InjectScopeIntoSql(cleanSql, scopeDict)

    isSafe = GM_IsSafeSelect(injectedSql)
    hasUnresolved = GM_HasUnresolvedEnginePlaceholders(injectedSql)

    If Not isSafe Then
        outError = "NOT_SAFE_SELECT"
        GM_BuildExecutableSqlByUrl = "SELECT '" & msgSqlNotSafe & "' AS Mensaje;"
        Exit Function
    End If

    If hasUnresolved Then
        outError = "UNRESOLVED_ENGINE_PLACEHOLDERS"
        GM_BuildExecutableSqlByUrl = "SELECT '" & msgSqlPlaceholders & "' AS Mensaje;"
        Exit Function
    End If

    If Len(Trim(injectedSql)) = 0 Then
        outError = "SQL_EMPTY_AFTER_BUILD"
        GM_BuildExecutableSqlByUrl = "SELECT '" & msgSqlEmpty & "' AS Mensaje;"
        Exit Function
    End If

    outStatus = "OK"
    GM_BuildExecutableSqlByUrl = injectedSql
End Function

Function GM_BuildExecutableSql(reportId, scopeDict, ByRef outStatus, ByRef outError)
    GM_BuildExecutableSql = GM_BuildExecutableSqlByUrl(GetReportSqlUrlByID(reportId), scopeDict, outStatus, outError)
End Function


'------------------------------------------------------------------------------
' ENGINE ORCHESTRATION - RUNTIME CONTROLLER
'------------------------------------------------------------------------------
' Entrypoint that resolves context, executes runtime, and emits UI payloads.
sCurrentDBName = "-"
sConnStringRaw = ""
sFlowwwHost = "-"
sDbVersion = "-"
sSessionUid = "" : sSessionUnm = "" : sSessionUlc = "" : sSessionUcd = "" : sSessionCid = ""
sSessionSda = "" : sSessionSid = "" : sSessionCtz = "" : sSessionUip = "" : sSessionUcl = "" : sSessionUlw = ""
sMetaIPMasked = "-"
sSelectedUserFilter = ""
sUserFilterHTML = ""
sFirstUserID = ""
sFirstUserName = ""
EffectiveUserName = "-"
CtxUserName = "-"
bSelectedClinicFound = False
AuthIsSuperAdmin = False
sCtxClientNow = ""
sCtxClientTZ = ""
vCtxClientNow = ""
vAuthSessionStart = ""
nCtxClientDeltaSec = 0
bShowSqlCodeLink = False
bIsSqlPopupRequested = False
bSqlPopupAutoOpen = False
bIsSqlPopupApiRequested = False
bIsConfigPopupRequested = False
bConfigPopupAutoOpen = False
bIsConfigPopupApiRequested = False
sSqlPopupKind = "data"
sConfigPopupKind = "reports"
bIsDevOnlyPopupFlow = False
nLoaderSafetyTimeoutMs = 25000
MasterConfigJsonText = ""
MasterSecuritySuperAdminUserIDs = "231"
MasterSecurityDeveloperToolUserIDs = "378"
MasterSecurityDeveloperUserIDs = ""
MasterSecuritySuperAdminProfileIDs = "0,1,6,16"
MasterDateStandardUserMinMonthOffset = -3
MasterDateStandardUserMaxMonthOffset = 1
MasterDateSuperAdminMinDateISO = "2025-10-01"
MasterDateSuperAdminMaxMonthOffset = 3
MasterEngineDefaultDateRangeDays = 7
MasterEngineActive = True
MasterCacheEnable = True
MasterCacheConfigTTLSeconds = 300
MasterCacheReportsTTLSeconds = 300
MasterPerfHttpFetchTimeoutMs = 5000
MasterPerfHttpLogTimeoutMs = 2000
MasterPerfSlowQueryThresholdMs = 2000
MasterPerfSlowRenderThresholdMs = 1000
MasterSqlTimeoutSeconds = 15
MasterSqlMaxRows = 50000
MasterKpiTimeoutMs = 5000
MasterKpiMaxItems = 5
MasterGraphTimeoutMs = 5000
MasterGraphMaxPoints = 200
MasterEngineEnvironment = "production"
MasterEngineEnableHomeDashboard = True
MasterEngineCorporateClinicOrder = "1,2,3,4,5,6,12,8,7,13,9,10"
PathLogEndpointValue = GM_LOG_WEBAPP_URL
EngineVersionValue = GM_ENGINE_VERSION
PathMasterConfigValue = GM_MASTER_CONFIG_DEFAULT_URL
PathModulesConfigValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_MODULES_CONFIG_PLACEHOLDER.json"
PathHomeConfigValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_HOME_CONFIG_PLACEHOLDER.json"
PathReportsFolderValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/"
PathIconsFolderValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/"
PathLoaderIconValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/ICON_LOADER_GM.png"
PathMaintenanceIconValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/ICON_MAINTENANCE_GM.png"
UIFooterDisclaimerText3Value = "Aclaraciones:"
MsgStdMaintenanceTitle = "Plataforma en mantenimiento"
MsgStdMaintenanceBody = "Estamos actualizando la configuracion del sistema."
MsgStdMaintenanceRetry = "Intente nuevamente en unos minutos."
MsgStdReportUndefined = "Reporte no definido"
MsgStdReportUnavailable = "Reporte no disponible"
MsgStdTableNoData = "Sin datos para los filtros seleccionados."
MsgStdKpiNoData = "Sin KPIs"
MsgStdGraphNoData = "No hay datos suficientes para generar la grafica con los filtros seleccionados."
MsgStdGraphSqlBuildFailed = "No fue posible generar la grafica en este momento."
MsgStdGraphLibLoadFailed = "No fue posible cargar el componente de visualizacion."
MsgStdGraphCanvasInitFailed = "No fue posible inicializar la grafica."
MsgStdSqlNotSafe = "La consulta solicitada no pudo ser procesada por razones de seguridad."
MsgStdSqlPlaceholdersUnresolved = "La consulta no pudo completarse correctamente."
MsgStdSqlEmptyAfterBuild = "La consulta no genero resultados validos."
MsgStdNotAvailable = "Informacion no disponible"
MsgStdNA = "No disponible"
MsgAdmNoActiveUsers = "Sin usuarios activos"
MsgAdmLogLastActionTitle = "Log ultima accion"
MsgDevFiltersNotConfigured = "Reporte {ReportName} no tiene filtros configurados."
MsgDevDataSqlNotConfigured = "Reporte {ReportName} no tiene SQL de datos configurado."
MsgDevKpisNotConfigured = "Reporte {ReportName} no tiene KPIs configurados."
MsgDevGraphNotConfigured = "Reporte {ReportName} no tiene graficos configurados."
MsgDevSqlEmptyRender = "-- SQL loaded, but no executable content remained."
MsgDevSqlLoading = "Loading SQL preview..."
MsgDevSqlPopupErrorTitle = "SQL popup error"
MsgDevSqlPopupErrorBody = "Unable to load SQL preview without refreshing data"
MsgDevConfigLoading = "Loading config preview..."
MsgDevConfigPopupErrorTitle = "Config popup error"
MsgDevConfigPopupErrorBody = "Unable to load config preview without refreshing data"
WAOpsTemplateValue = "Hola! Necesito aclaraciones con el reporte {ReportTitle}. Gracias!"
WASupportTemplateValue = "Hola! Necesito soporte - {MetadataText}"
GraphDataJson = "[]"
GraphConfigJson = GM_GetDefaultGraphConfigJson()
SqlPopupUrl = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/Catalogo_Sucursales.sql"
SqlPopupRaw = ""
SqlPopupClean = ""
SqlPopupContent = ""
SqlPopupStatusIcon = ""
SqlPopupBuildStatus = ""
SqlPopupBuildError = ""
ConfigPopupUrl = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_REPORT_CONFIG.json"
ConfigPopupContent = ""
ConfigPopupStatusIcon = ""
ConfigPopupBuildStatus = ""
ConfigPopupBuildError = ""
TxtLogExecutionTimestamp = ""
sReqFilter1IDs = "" : sReqFilter2IDs = "" : sReqFilter3IDs = ""
sFilter1ID = "0" : sFilter1IDs = "" : sFilter1Title = "Filtro 1" : sFilter1Name = "-" : sFilter1OptionsHTML = ""
sFilter2ID = "0" : sFilter2IDs = "" : sFilter2Title = "Filtro 2" : sFilter2Name = "-" : sFilter2OptionsHTML = ""
sFilter3ID = "0" : sFilter3IDs = "" : sFilter3Title = "Filtro 3" : sFilter3Name = "-" : sFilter3OptionsHTML = ""
RequestStartTimer = Timer

On Error Resume Next
sFlowwwHost = GetServerVarText("HTTP_HOST")
sSessionUid = Trim(CStr(Session("uid")))
sSessionUnm = Trim(CStr(Session("unm")))
sSessionUlc = Trim(CStr(Session("ulc")))
sSessionUcd = Trim(CStr(Session("ucd")))
sSessionCid = Trim(CStr(Session("cid")))
sSessionSda = Trim(CStr(Session("sda")))
sSessionSid = Trim(CStr(Session("sid")))
sSessionCtz = Trim(CStr(Session("ctz")))
sSessionUip = Trim(CStr(Session("uip")))
sSessionUcl = Trim(CStr(Session("ucl")))
sSessionUlw = Trim(CStr(Session("ulw")))
If Len(Trim(sFlowwwHost)) = 0 Then sFlowwwHost = "-"
Err.Clear
On Error GoTo 0

On Error Resume Next
If Not objConnection Is Nothing Then
    sCurrentDBName = Trim(CStr(objConnection.Properties("Initial Catalog")))
    sConnStringRaw = CStr(objConnection.ConnectionString)
End If
Err.Clear
On Error GoTo 0

If Len(Trim(sCurrentDBName)) = 0 Then sCurrentDBName = GetConnStringValue(sConnStringRaw, "DATABASE")
If Len(Trim(sCurrentDBName)) = 0 Then sCurrentDBName = GetConnStringValue(sConnStringRaw, "INITIAL CATALOG")
If Len(Trim(sCurrentDBName)) = 0 Then sCurrentDBName = GetConnStringValue(sConnStringRaw, "DBQ")
If Len(Trim(sCurrentDBName)) = 0 Then sCurrentDBName = "-"
If sCurrentDBName = "-" Then
    sDbInfoSQL =    "SELECT " & _
                    "IFNULL(DATABASE(), '-') AS CurrentDBName " & _
                    "LIMIT 1;"

    Set rsDbInfo = Server.CreateObject("ADODB.Recordset")
    On Error Resume Next
    rsDbInfo.Open sDbInfoSQL, objConnection, adOpenForwardOnly, adLockReadOnly
    If Err.Number = 0 And Not rsDbInfo.EOF Then
        If sCurrentDBName = "-" Then sCurrentDBName = CStr(rsDbInfo("CurrentDBName"))
    End If
    Err.Clear
    On Error GoTo 0
    If Not rsDbInfo Is Nothing Then
        If rsDbInfo.State = 1 Then rsDbInfo.Close
        Set rsDbInfo = Nothing
    End If
End If
sDbVersionSQL = "SELECT IFNULL(@@version, '-') AS DBVersion LIMIT 1;"
Set rsDbVersion = Server.CreateObject("ADODB.Recordset")
On Error Resume Next
rsDbVersion.Open sDbVersionSQL, objConnection, adOpenForwardOnly, adLockReadOnly
If Err.Number = 0 And Not rsDbVersion.EOF Then
    sDbVersion = CStr(rsDbVersion("DBVersion"))
End If
Err.Clear
On Error GoTo 0
If Not rsDbVersion Is Nothing Then
    If rsDbVersion.State = 1 Then rsDbVersion.Close
    Set rsDbVersion = Nothing
End If
MasterConfigJsonText = GM_GetMasterConfigJson()
' Runtime aligned to Engine/Auth/Effective/Scope architecture.
EngineName = "BI Growmetrica"
EngineConfigName = "SKG Catalogs"
EngineTitle = "Skingroup Control Center - Catalogos"
EngineAllowedClinicIDs = "1,2,3,4,5,6,12,8,7,13,9,10"
PolicyLink_SKG = "https://docs.google.com/document/d/16zsxfoIJ6RwNUg_uT3FYQj27glKynJw-hlEsAFPNrW8/edit?usp=sharing"
OperationsWA = "528186850485"
SupportWA = "528114716090"
DisclaimerText1 = "Resultados del periodo en curso, sujetos a consolidacion operativa."
DisclaimerText2 = "Informacion de la operacion registrada en Flowww (MPro no incluido)"
AuthorText = "Disenado y creado por Growmetrica para SKG"
Report_Config_File = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_REPORT_CONFIG.json"
If GM_IsValidMasterConfigJson(MasterConfigJsonText) Then
    EngineName = GM_MasterCfgGetString(MasterConfigJsonText, "EngineName", EngineName)
    EngineTitle = GM_MasterCfgGetString(MasterConfigJsonText, "UITitle", EngineTitle)
    EngineAllowedClinicIDs = GM_MasterCfgGetCsvInt(MasterConfigJsonText, "SecurityAllowedClinicIDs", EngineAllowedClinicIDs)
    PolicyLink_SKG = GM_MasterCfgGetString(MasterConfigJsonText, "PathPolicyEndpoint", PolicyLink_SKG)
    OperationsWA = GM_MasterCfgGetString(MasterConfigJsonText, "WAOpsNumber", OperationsWA)
    SupportWA = GM_MasterCfgGetString(MasterConfigJsonText, "WASupportNumber", SupportWA)
    DisclaimerText1 = GM_MasterCfgGetString(MasterConfigJsonText, "UIFooterDisclaimerText1", DisclaimerText1)
    DisclaimerText2 = GM_MasterCfgGetString(MasterConfigJsonText, "UIFooterDisclaimerText2", DisclaimerText2)
    AuthorText = GM_MasterCfgGetString(MasterConfigJsonText, "UIFooterBrandingText", AuthorText)
    Report_Config_File = GM_MasterCfgGetString(MasterConfigJsonText, "PathReportsConfig", Report_Config_File)
    PathLogEndpointValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathLogEndpoint", PathLogEndpointValue)
    EngineVersionValue = GM_MasterCfgGetString(MasterConfigJsonText, "EngineVersion", EngineVersionValue)
    PathMasterConfigValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathMasterConfig", PathMasterConfigValue)
    PathModulesConfigValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathModulesConfig", PathModulesConfigValue)
    PathHomeConfigValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathHomeConfig", PathHomeConfigValue)
    PathReportsFolderValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathReportsFolder", PathReportsFolderValue)
    PathIconsFolderValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathIconsFolder", PathIconsFolderValue)
    PathLoaderIconValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathLoaderIcon", PathLoaderIconValue)
    PathMaintenanceIconValue = GM_MasterCfgGetString(MasterConfigJsonText, "PathMaintenanceIcon", PathMaintenanceIconValue)
    UIFooterDisclaimerText3Value = GM_MasterCfgGetString(MasterConfigJsonText, "UIFooterDisclaimerText3", UIFooterDisclaimerText3Value)
    MsgStdMaintenanceTitle = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdMaintenanceTitle", MsgStdMaintenanceTitle)
    MsgStdMaintenanceBody = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdMaintenanceBody", MsgStdMaintenanceBody)
    MsgStdMaintenanceRetry = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdMaintenanceRetry", MsgStdMaintenanceRetry)
    MsgStdReportUndefined = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdReportUndefined", MsgStdReportUndefined)
    MsgStdReportUnavailable = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdReportUnavailable", MsgStdReportUnavailable)
    MsgStdTableNoData = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdTableNoData", MsgStdTableNoData)
    MsgStdKpiNoData = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdKpiNoData", MsgStdKpiNoData)
    MsgStdGraphNoData = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdGraphNoData", MsgStdGraphNoData)
    MsgStdGraphSqlBuildFailed = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdGraphSqlBuildFailed", MsgStdGraphSqlBuildFailed)
    MsgStdGraphLibLoadFailed = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdGraphLibLoadFailed", MsgStdGraphLibLoadFailed)
    MsgStdGraphCanvasInitFailed = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdGraphCanvasInitFailed", MsgStdGraphCanvasInitFailed)
    MsgStdSqlNotSafe = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdSqlNotSafe", MsgStdSqlNotSafe)
    MsgStdSqlPlaceholdersUnresolved = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdSqlPlaceholdersUnresolved", MsgStdSqlPlaceholdersUnresolved)
    MsgStdSqlEmptyAfterBuild = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdSqlEmptyAfterBuild", MsgStdSqlEmptyAfterBuild)
    MsgStdNotAvailable = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdNotAvailable", MsgStdNotAvailable)
    MsgStdNA = GM_MasterCfgGetString(MasterConfigJsonText, "MsgStdNA", MsgStdNA)
    MsgAdmNoActiveUsers = GM_MasterCfgGetString(MasterConfigJsonText, "MsgAdmNoActiveUsers", MsgAdmNoActiveUsers)
    MsgAdmLogLastActionTitle = GM_MasterCfgGetString(MasterConfigJsonText, "MsgAdmLogLastActionTitle", MsgAdmLogLastActionTitle)
    MsgDevFiltersNotConfigured = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevFiltersNotConfigured", MsgDevFiltersNotConfigured)
    MsgDevDataSqlNotConfigured = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevDataSqlNotConfigured", MsgDevDataSqlNotConfigured)
    MsgDevKpisNotConfigured = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevKpisNotConfigured", MsgDevKpisNotConfigured)
    MsgDevGraphNotConfigured = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevGraphNotConfigured", MsgDevGraphNotConfigured)
    MsgDevSqlEmptyRender = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevSqlEmptyRender", MsgDevSqlEmptyRender)
    MsgDevSqlLoading = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevSqlLoading", MsgDevSqlLoading)
    MsgDevSqlPopupErrorTitle = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevSqlPopupErrorTitle", MsgDevSqlPopupErrorTitle)
    MsgDevSqlPopupErrorBody = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevSqlPopupErrorBody", MsgDevSqlPopupErrorBody)
    MsgDevConfigLoading = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevConfigLoading", MsgDevConfigLoading)
    MsgDevConfigPopupErrorTitle = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevConfigPopupErrorTitle", MsgDevConfigPopupErrorTitle)
    MsgDevConfigPopupErrorBody = GM_MasterCfgGetString(MasterConfigJsonText, "MsgDevConfigPopupErrorBody", MsgDevConfigPopupErrorBody)
    WAOpsTemplateValue = GM_MasterCfgGetString(MasterConfigJsonText, "WAOpsTemplate", WAOpsTemplateValue)
    WASupportTemplateValue = GM_MasterCfgGetString(MasterConfigJsonText, "WASupportTemplate", WASupportTemplateValue)

    MasterSecuritySuperAdminUserIDs = GM_MasterCfgGetCsvInt(MasterConfigJsonText, "SecuritySuperAdminUserIDs", MasterSecuritySuperAdminUserIDs)
    MasterSecurityDeveloperToolUserIDs = GM_MasterCfgGetCsvInt(MasterConfigJsonText, "SecurityDeveloperToolUserIDs", MasterSecurityDeveloperToolUserIDs)
    MasterSecurityDeveloperUserIDs = GM_MasterCfgGetCsvInt(MasterConfigJsonText, "SecurityDeveloperUserIDs", MasterSecurityDeveloperUserIDs)
    MasterSecuritySuperAdminProfileIDs = GM_MasterCfgGetCsvInt(MasterConfigJsonText, "SecuritySuperAdminProfileIDs", MasterSecuritySuperAdminProfileIDs)

    MasterCacheEnable = GM_MasterCfgGetBool(MasterConfigJsonText, "CacheEnable", MasterCacheEnable)
    MasterCacheConfigTTLSeconds = GM_MasterCfgGetInt(MasterConfigJsonText, "CacheConfigTTLSeconds", MasterCacheConfigTTLSeconds)
    MasterCacheReportsTTLSeconds = GM_MasterCfgGetInt(MasterConfigJsonText, "CacheReportsTTLSeconds", MasterCacheReportsTTLSeconds)
    MasterPerfHttpFetchTimeoutMs = GM_MasterCfgGetInt(MasterConfigJsonText, "PerfHttpFetchTimeoutMs", MasterPerfHttpFetchTimeoutMs)
    MasterPerfHttpLogTimeoutMs = GM_MasterCfgGetInt(MasterConfigJsonText, "PerfHttpLogTimeoutMs", MasterPerfHttpLogTimeoutMs)
    MasterPerfSlowQueryThresholdMs = GM_MasterCfgGetInt(MasterConfigJsonText, "PerfSlowQueryThresholdMs", MasterPerfSlowQueryThresholdMs)
    MasterPerfSlowRenderThresholdMs = GM_MasterCfgGetInt(MasterConfigJsonText, "PerfSlowRenderThresholdMs", MasterPerfSlowRenderThresholdMs)
    MasterSqlTimeoutSeconds = GM_MasterCfgGetInt(MasterConfigJsonText, "SqlTimeoutSeconds", MasterSqlTimeoutSeconds)
    MasterSqlMaxRows = GM_MasterCfgGetInt(MasterConfigJsonText, "SqlMaxRows", MasterSqlMaxRows)
    MasterKpiTimeoutMs = GM_MasterCfgGetInt(MasterConfigJsonText, "KpiTimeoutMs", MasterKpiTimeoutMs)
    MasterKpiMaxItems = GM_MasterCfgGetInt(MasterConfigJsonText, "KpiMaxItems", MasterKpiMaxItems)
    MasterGraphTimeoutMs = GM_MasterCfgGetInt(MasterConfigJsonText, "GraphTimeoutMs", MasterGraphTimeoutMs)
    MasterGraphMaxPoints = GM_MasterCfgGetInt(MasterConfigJsonText, "GraphMaxPoints", MasterGraphMaxPoints)
    MasterEngineEnvironment = GM_MasterCfgGetString(MasterConfigJsonText, "EngineEnvironment", MasterEngineEnvironment)
    MasterEngineEnableHomeDashboard = GM_MasterCfgGetBool(MasterConfigJsonText, "EngineEnableHomeDashboard", MasterEngineEnableHomeDashboard)
    MasterEngineCorporateClinicOrder = GM_MasterCfgGetCsvInt(MasterConfigJsonText, "EngineCorporateClinicOrder", MasterEngineCorporateClinicOrder)

    MasterDateStandardUserMinMonthOffset = GM_MasterCfgGetInt(MasterConfigJsonText, "DateStandardUserMinMonthOffset", MasterDateStandardUserMinMonthOffset)
    MasterDateStandardUserMaxMonthOffset = GM_MasterCfgGetInt(MasterConfigJsonText, "DateStandardUserMaxMonthOffset", MasterDateStandardUserMaxMonthOffset)
    MasterDateSuperAdminMinDateISO = GM_MasterCfgGetIsoDate(MasterConfigJsonText, "DateSuperAdminMinDate", MasterDateSuperAdminMinDateISO)
    MasterDateSuperAdminMaxMonthOffset = GM_MasterCfgGetInt(MasterConfigJsonText, "DateSuperAdminMaxMonthOffset", MasterDateSuperAdminMaxMonthOffset)
    MasterEngineDefaultDateRangeDays = GM_MasterCfgGetInt(MasterConfigJsonText, "EngineDefaultDateRangeDays", MasterEngineDefaultDateRangeDays)
    MasterEngineActive = GM_MasterCfgGetBool(MasterConfigJsonText, "EngineActive", MasterEngineActive)
End If

EngineAllowedClinicIDs = CsvNormalizeIntList(EngineAllowedClinicIDs)
If Len(EngineAllowedClinicIDs) = 0 Then EngineAllowedClinicIDs = "1,2,3,4,5,6,12,8,7,13,9,10"
MasterSecuritySuperAdminUserIDs = CsvNormalizeIntList(MasterSecuritySuperAdminUserIDs)
If Len(MasterSecuritySuperAdminUserIDs) = 0 Then MasterSecuritySuperAdminUserIDs = "231"
MasterSecurityDeveloperToolUserIDs = CsvNormalizeIntList(MasterSecurityDeveloperToolUserIDs)
If Len(MasterSecurityDeveloperToolUserIDs) = 0 Then MasterSecurityDeveloperToolUserIDs = "378"
MasterSecurityDeveloperUserIDs = CsvNormalizeIntList(MasterSecurityDeveloperUserIDs)
MasterSecuritySuperAdminProfileIDs = CsvNormalizeIntList(MasterSecuritySuperAdminProfileIDs)
If Len(MasterSecuritySuperAdminProfileIDs) = 0 Then MasterSecuritySuperAdminProfileIDs = "0,1,6,16"
MasterEngineCorporateClinicOrder = CsvNormalizeIntList(MasterEngineCorporateClinicOrder)
If Len(MasterEngineCorporateClinicOrder) = 0 Then MasterEngineCorporateClinicOrder = EngineAllowedClinicIDs
If MasterEngineDefaultDateRangeDays <= 0 Then MasterEngineDefaultDateRangeDays = 7
If MasterDateSuperAdminMaxMonthOffset < 0 Then MasterDateSuperAdminMaxMonthOffset = 0
If MasterCacheConfigTTLSeconds < 0 Then MasterCacheConfigTTLSeconds = 0
If MasterCacheReportsTTLSeconds < 0 Then MasterCacheReportsTTLSeconds = 0
If MasterPerfHttpFetchTimeoutMs <= 0 Then MasterPerfHttpFetchTimeoutMs = 5000
If MasterPerfHttpLogTimeoutMs <= 0 Then MasterPerfHttpLogTimeoutMs = 2000
If MasterSqlTimeoutSeconds <= 0 Then MasterSqlTimeoutSeconds = 15
If MasterSqlTimeoutSeconds > 15 Then MasterSqlTimeoutSeconds = 15
If MasterSqlMaxRows <= 0 Then MasterSqlMaxRows = 50000
If MasterSqlMaxRows > 50000 Then MasterSqlMaxRows = 50000
If MasterKpiTimeoutMs <= 0 Then MasterKpiTimeoutMs = 5000
If MasterKpiMaxItems <= 0 Then MasterKpiMaxItems = 5
If MasterGraphTimeoutMs <= 0 Then MasterGraphTimeoutMs = 5000
If MasterGraphMaxPoints <= 0 Then MasterGraphMaxPoints = 200
MasterDateSuperAdminMinDateISO = ToIsoDate(ParseIsoDateOrDefault(MasterDateSuperAdminMinDateISO, DateSerial(2025, 10, 1)))
If Len(Trim(PathMasterConfigValue)) = 0 Then PathMasterConfigValue = GM_MASTER_CONFIG_DEFAULT_URL
If Len(Trim(PathModulesConfigValue)) = 0 Then PathModulesConfigValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_MODULES_CONFIG_PLACEHOLDER.json"
If Len(Trim(PathHomeConfigValue)) = 0 Then PathHomeConfigValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_HOME_CONFIG_PLACEHOLDER.json"
If Len(Trim(PathReportsFolderValue)) = 0 Then PathReportsFolderValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/"
If Len(Trim(PathIconsFolderValue)) = 0 Then PathIconsFolderValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/"
If Len(Trim(PathLoaderIconValue)) = 0 Then PathLoaderIconValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/ICON_LOADER_GM.png"
If Len(Trim(PathMaintenanceIconValue)) = 0 Then PathMaintenanceIconValue = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/ICON_MAINTENANCE_GM.png"
If Len(Trim(MasterEngineEnvironment)) = 0 Then MasterEngineEnvironment = "production"
If Len(Trim(UIFooterDisclaimerText3Value)) = 0 Then UIFooterDisclaimerText3Value = "Aclaraciones:"
If Len(Trim(MsgStdMaintenanceTitle)) = 0 Then MsgStdMaintenanceTitle = "Plataforma en mantenimiento"
If Len(Trim(MsgStdMaintenanceBody)) = 0 Then MsgStdMaintenanceBody = "Estamos actualizando la configuracion del sistema."
If Len(Trim(MsgStdMaintenanceRetry)) = 0 Then MsgStdMaintenanceRetry = "Intente nuevamente en unos minutos."
If Len(Trim(MsgStdReportUndefined)) = 0 Then MsgStdReportUndefined = "Reporte no definido"
If Len(Trim(MsgStdReportUnavailable)) = 0 Then MsgStdReportUnavailable = "Reporte no disponible"
If Len(Trim(MsgStdTableNoData)) = 0 Then MsgStdTableNoData = "Sin datos para los filtros seleccionados."
If Len(Trim(MsgStdKpiNoData)) = 0 Then MsgStdKpiNoData = "Sin KPIs"
If Len(Trim(MsgStdGraphNoData)) = 0 Then MsgStdGraphNoData = "No hay datos suficientes para generar la grafica con los filtros seleccionados."
If Len(Trim(MsgStdGraphSqlBuildFailed)) = 0 Then MsgStdGraphSqlBuildFailed = "No fue posible generar la grafica en este momento."
If Len(Trim(MsgStdGraphLibLoadFailed)) = 0 Then MsgStdGraphLibLoadFailed = "No fue posible cargar el componente de visualizacion."
If Len(Trim(MsgStdGraphCanvasInitFailed)) = 0 Then MsgStdGraphCanvasInitFailed = "No fue posible inicializar la grafica."
If Len(Trim(MsgStdSqlNotSafe)) = 0 Then MsgStdSqlNotSafe = "La consulta solicitada no pudo ser procesada por razones de seguridad."
If Len(Trim(MsgStdSqlPlaceholdersUnresolved)) = 0 Then MsgStdSqlPlaceholdersUnresolved = "La consulta no pudo completarse correctamente."
If Len(Trim(MsgStdSqlEmptyAfterBuild)) = 0 Then MsgStdSqlEmptyAfterBuild = "La consulta no genero resultados validos."
If Len(Trim(MsgStdNotAvailable)) = 0 Then MsgStdNotAvailable = "Informacion no disponible"
If Len(Trim(MsgStdNA)) = 0 Then MsgStdNA = "No disponible"
If Len(Trim(MsgAdmNoActiveUsers)) = 0 Then MsgAdmNoActiveUsers = "Sin usuarios activos"
If Len(Trim(MsgAdmLogLastActionTitle)) = 0 Then MsgAdmLogLastActionTitle = "Log ultima accion"
If Len(Trim(MsgDevFiltersNotConfigured)) = 0 Then MsgDevFiltersNotConfigured = "Reporte {ReportName} no tiene filtros configurados."
If Len(Trim(MsgDevDataSqlNotConfigured)) = 0 Then MsgDevDataSqlNotConfigured = "Reporte {ReportName} no tiene SQL de datos configurado."
If Len(Trim(MsgDevKpisNotConfigured)) = 0 Then MsgDevKpisNotConfigured = "Reporte {ReportName} no tiene KPIs configurados."
If Len(Trim(MsgDevGraphNotConfigured)) = 0 Then MsgDevGraphNotConfigured = "Reporte {ReportName} no tiene graficos configurados."
If Len(Trim(MsgDevSqlEmptyRender)) = 0 Then MsgDevSqlEmptyRender = "-- SQL loaded, but no executable content remained."
If Len(Trim(MsgDevSqlLoading)) = 0 Then MsgDevSqlLoading = "Loading SQL preview..."
If Len(Trim(MsgDevSqlPopupErrorTitle)) = 0 Then MsgDevSqlPopupErrorTitle = "SQL popup error"
If Len(Trim(MsgDevSqlPopupErrorBody)) = 0 Then MsgDevSqlPopupErrorBody = "Unable to load SQL preview without refreshing data"
If Len(Trim(MsgDevConfigLoading)) = 0 Then MsgDevConfigLoading = "Loading config preview..."
If Len(Trim(MsgDevConfigPopupErrorTitle)) = 0 Then MsgDevConfigPopupErrorTitle = "Config popup error"
If Len(Trim(MsgDevConfigPopupErrorBody)) = 0 Then MsgDevConfigPopupErrorBody = "Unable to load config preview without refreshing data"
If Len(Trim(WAOpsTemplateValue)) = 0 Then WAOpsTemplateValue = "Hola! Necesito aclaraciones con el reporte {ReportTitle}. Gracias!"
If Len(Trim(WASupportTemplateValue)) = 0 Then WASupportTemplateValue = "Hola! Necesito soporte - {MetadataText}"

If Not MasterEngineActive Then
    Call GM_PlatformMaintenance()
    Response.End
End If

ConfigPopupUrl = Trim(CStr(Report_Config_File))
If Len(Trim(ConfigPopupUrl)) = 0 Then ConfigPopupUrl = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_REPORT_CONFIG.json"
EngineReportCount = GM_GetReportConfigMaxID()
If ToLongOrZero(EngineReportCount) <= 0 Then EngineReportCount = 2

AuthSessionID = Trim(CStr(sSessionSid))
If Len(AuthSessionID) = 0 Then
    On Error Resume Next
    AuthSessionID = CStr(Session.SessionID)
    Err.Clear
    On Error GoTo 0
End If
AuthSessionStart = Trim(CStr(sSessionSda))
AuthSessionTZ = Trim(CStr(sSessionCtz))
sCtxClientNow = Trim(CStr(Request("ctx_client_now")))
sCtxClientTZ = Trim(CStr(Request("ctx_client_tz")))

If Len(Trim(sCtxClientTZ)) > 0 Then AuthSessionTZ = GmAsciiSafe(sCtxClientTZ)
vCtxClientNow = ParseDateTimeFlexibleOrBlank(sCtxClientNow)
If IsDate(vCtxClientNow) Then
    CtxTimeStamp = ToIsoDateTime(CDate(vCtxClientNow))
Else
    CtxTimeStamp = ToIsoDateTime(Now())
End If

vAuthSessionStart = ParseDateTimeFlexibleOrBlank(AuthSessionStart)
If IsDate(vAuthSessionStart) And IsDate(vCtxClientNow) Then
    nCtxClientDeltaSec = DateDiff("s", Now(), CDate(vCtxClientNow))
    AuthSessionStart = FormatToDMYDateTime(ToIsoDateTime(DateAdd("s", nCtxClientDeltaSec, CDate(vAuthSessionStart))))
ElseIf IsDate(vAuthSessionStart) Then
    AuthSessionStart = FormatToDMYDateTime(ToIsoDateTime(CDate(vAuthSessionStart)))
Else
    AuthSessionStart = FormatToDMYDateTime(AuthSessionStart)
End If
If Len(Trim(AuthSessionStart)) = 0 Or AuthSessionStart = "-" Then AuthSessionStart = FormatToDMYDateTime(ToIsoDateTime(Now()))
AuthUserID = Trim(CStr(sSessionUid))
If Len(AuthUserID) = 0 Then
    On Error Resume Next
    AuthUserID = CStr(ToLongOrZero(Session(SES_USERID)))
    If ToLongOrZero(AuthUserID) = 0 Then AuthUserID = CStr(ToLongOrZero(Session("SES_USERID")))
    Err.Clear
    On Error GoTo 0
End If
bShowSqlCodeLink = CsvContainsInt(MasterSecurityDeveloperToolUserIDs, AuthUserID)
AuthUserName = Trim(CStr(sSessionUnm))
AuthProfileID = Trim(CStr(sSessionUcl))
AuthProfileDesc = Trim(CStr(sSessionUcd))
AuthClinicID = Trim(CStr(sSessionCid))
If Len(AuthClinicID) = 0 Then
    On Error Resume Next
    AuthClinicID = CStr(ToLongOrZero(Session(SES_USER_CLINICID)))
    If ToLongOrZero(AuthClinicID) = 0 Then AuthClinicID = CStr(ToLongOrZero(Session("SES_USER_CLINICID")))
    Err.Clear
    On Error GoTo 0
End If
AuthPermClinicsIDs = CsvNormalizeIntList(sSessionUlc)
If Len(AuthPermClinicsIDs) = 0 Then AuthPermClinicsIDs = CsvNormalizeIntList(sSessionUlw)

AuthIsSuperAdmin = IsSuperAdminCode(AuthProfileID) Or CsvContainsInt(MasterSecuritySuperAdminUserIDs, AuthUserID)
AuthAllowedClinicIDs = CsvIntersect(AuthPermClinicsIDs, EngineAllowedClinicIDs)
If AuthIsSuperAdmin And Len(AuthAllowedClinicIDs) = 0 Then AuthAllowedClinicIDs = CsvNormalizeIntList(EngineAllowedClinicIDs)
If Len(AuthAllowedClinicIDs) = 0 And IsNumeric(AuthClinicID) Then AuthAllowedClinicIDs = CStr(CLng(AuthClinicID))

If ToLongOrZero(AuthClinicID) > 0 Then
    sSQL = "SELECT IFNULL(ClinicCommercialName, '-') AS ClinicCommercialName " & _
           "FROM x_config_clinics WHERE ClinicID = " & ToLongOrZero(AuthClinicID) & " LIMIT 1;"
    Set rsClinicLookup = Gm_OpenRs(sSQL)
    If Not rsClinicLookup Is Nothing Then
        If Not rsClinicLookup.EOF Then AuthClinicName = CStr(rsClinicLookup("ClinicCommercialName"))
        If rsClinicLookup.State = 1 Then rsClinicLookup.Close
        Set rsClinicLookup = Nothing
    End If
End If
If Len(Trim(AuthClinicName)) = 0 Then AuthClinicName = "-"

If ToLongOrZero(AuthUserID) > 0 Then
    sSQL = "SELECT " & _
           "IFNULL(u.UserName, '-') AS SessionUserName, " & _
           "IFNULL(CONCAT('', w.WorkPlaceUserClassID), '-') AS SessionUserClassID, " & _
           "IFNULL(uc.UserClassDesc, '-') AS SessionUserClassDesc " & _
           "FROM __x_config_users_view u " & _
           "LEFT JOIN __x_config_workplaces_view w ON w.WorkPlaceUserID = u.UserID AND w.WorkPlaceClinicID = " & ToLongOrZero(AuthClinicID) & " " & _
           "LEFT JOIN x_config_userclasses uc ON uc.UserClassID = w.WorkPlaceUserClassID " & _
           "WHERE u.UserID = " & ToLongOrZero(AuthUserID) & " LIMIT 1;"
    Set rsSessionMeta = Gm_OpenRs(sSQL)
    If Not rsSessionMeta Is Nothing Then
        If Not rsSessionMeta.EOF Then
            If Len(Trim(AuthUserName)) = 0 Then AuthUserName = CStr(rsSessionMeta("SessionUserName"))
            If Len(Trim(AuthProfileID)) = 0 Then AuthProfileID = CStr(rsSessionMeta("SessionUserClassID"))
            If Len(Trim(AuthProfileDesc)) = 0 Then AuthProfileDesc = CStr(rsSessionMeta("SessionUserClassDesc"))
        End If
        If rsSessionMeta.State = 1 Then rsSessionMeta.Close
        Set rsSessionMeta = Nothing
    End If
End If
If Len(Trim(AuthUserName)) = 0 Then AuthUserName = "-"
If Len(Trim(AuthProfileID)) = 0 Then AuthProfileID = "-"
If Len(Trim(AuthProfileDesc)) = 0 Then AuthProfileDesc = "-"
AuthAllowedReportIDs = BuildAllowedReportIDsForProfile(AuthProfileID)

sSelectedUserFilter = Trim(Request("user_filter"))
If Len(sSelectedUserFilter) > 0 And IsNumeric(sSelectedUserFilter) Then
    sSelectedUserFilter = CStr(CLng(sSelectedUserFilter))
Else
    sSelectedUserFilter = ""
End If
EffectiveUserName = "-"
sUserFilterHTML = ""
sFirstUserID = ""
sFirstUserName = ""
bSelectedUserFound = False
If Len(Trim(sSelectedUserFilter)) = 0 Then sSelectedUserFilter = CStr(ToLongOrZero(AuthUserID))

sUsersSQL = "SELECT CONCAT('', UserID) AS UserID, IFNULL(UserName, '-') AS UserName " & _
            "FROM __x_config_users_view WHERE IFNULL(UserDisabled, 0) = 0 ORDER BY UserName ASC;"
If AuthIsSuperAdmin Then
    Set rsUsers = Gm_OpenRs(sUsersSQL)
    If Not rsUsers Is Nothing Then
        Do While Not rsUsers.EOF
            uidUser = CStr(rsUsers("UserID"))
            unameUser = CStr(rsUsers("UserName"))
            If Len(sFirstUserID) = 0 Then
                sFirstUserID = uidUser
                sFirstUserName = unameUser
            End If
            selectedUserAttr = ""
            If uidUser = sSelectedUserFilter Then
                selectedUserAttr = " selected"
                EffectiveUserName = unameUser
                bSelectedUserFound = True
            End If
            sUserFilterHTML = sUserFilterHTML & "<option value=""" & Server.HTMLEncode(uidUser) & """" & selectedUserAttr & ">" & Server.HTMLEncode(unameUser) & "</option>"
            rsUsers.MoveNext
        Loop
        If rsUsers.State = 1 Then rsUsers.Close
        Set rsUsers = Nothing
    End If
    If Not bSelectedUserFound Then
        sSelectedUserFilter = CStr(ToLongOrZero(AuthUserID))
        EffectiveUserName = AuthUserName
        If Len(Trim(sSelectedUserFilter)) > 0 And InStr(1, sUserFilterHTML, "value=""" & sSelectedUserFilter & """", vbTextCompare) > 0 Then
            sUserFilterHTML = Replace(sUserFilterHTML, "value=""" & sSelectedUserFilter & """", "value=""" & sSelectedUserFilter & """ selected", 1, 1, vbTextCompare)
            bSelectedUserFound = True
        End If
    End If
    If Not bSelectedUserFound And Len(Trim(sFirstUserID)) > 0 Then
        sSelectedUserFilter = sFirstUserID
        EffectiveUserName = sFirstUserName
        sUserFilterHTML = Replace(sUserFilterHTML, "value=""" & sFirstUserID & """", "value=""" & sFirstUserID & """ selected", 1, 1, vbTextCompare)
    End If
Else
    sSelectedUserFilter = CStr(ToLongOrZero(AuthUserID))
    EffectiveUserName = AuthUserName
    sUserFilterHTML = "<option value=""" & Server.HTMLEncode(sSelectedUserFilter) & """ selected>" & Server.HTMLEncode(EffectiveUserName) & "</option>"
End If
If Len(Trim(sSelectedUserFilter)) = 0 Then sSelectedUserFilter = "0"
If Len(Trim(EffectiveUserName)) = 0 Then EffectiveUserName = "-"
CtxUserName = EffectiveUserName
If Len(Trim(sUserFilterHTML)) = 0 Then sUserFilterHTML = "<option value=""0"" selected>" & Server.HTMLEncode(CStr(MsgAdmNoActiveUsers)) & "</option>"

EffectiveUserID = CStr(ToLongOrZero(sSelectedUserFilter))
EffectiveProfileID = AuthProfileID
EffectiveProfileDesc = AuthProfileDesc
If AuthIsSuperAdmin And ToLongOrZero(EffectiveUserID) > 0 Then
    sSQL = "SELECT uc.UserClassID AS EffectiveProfileID, IFNULL(uc.UserClassDesc,'-') AS EffectiveProfileDesc " & _
           "FROM __x_config_workplaces_view w " & _
           "INNER JOIN x_config_userclasses uc ON uc.UserClassID = CAST(w.WorkPlaceUserClassID AS UNSIGNED) " & _
           "WHERE w.WorkPlaceUserID = " & ToLongOrZero(EffectiveUserID) & " " & _
           "GROUP BY uc.UserClassID, uc.UserClassDesc, uc.UserClassRank " & _
           "ORDER BY uc.UserClassRank ASC, uc.UserClassID ASC LIMIT 1;"
    Set rsSelectedUser = Gm_OpenRs(sSQL)
    If Not rsSelectedUser Is Nothing Then
        If Not rsSelectedUser.EOF Then
            EffectiveProfileID = CStr(rsSelectedUser("EffectiveProfileID"))
            EffectiveProfileDesc = CStr(rsSelectedUser("EffectiveProfileDesc"))
        End If
        If rsSelectedUser.State = 1 Then rsSelectedUser.Close
        Set rsSelectedUser = Nothing
    End If
End If
If Len(Trim(EffectiveProfileID)) = 0 Then EffectiveProfileID = AuthProfileID
If Len(Trim(EffectiveProfileDesc)) = 0 Then EffectiveProfileDesc = AuthProfileDesc

If AuthIsSuperAdmin And ToLongOrZero(AuthUserID) = ToLongOrZero(EffectiveUserID) Then
    EffectiveAllowedReportIDs = "ALL"
    EffectiveAllowedClinicIDs = CsvNormalizeIntList(EngineAllowedClinicIDs)
Else
    EffectiveAllowedReportIDs = BuildAllowedReportIDsForProfile(EffectiveProfileID)
    EffectiveAllowedClinicIDs = AuthAllowedClinicIDs
End If
If Len(Trim(EffectiveAllowedClinicIDs)) = 0 Then EffectiveAllowedClinicIDs = AuthAllowedClinicIDs

sReportOptionsHTML = BuildReportOptionsHTML(EffectiveAllowedReportIDs)

sSelectedReportID = Trim(Request("report"))
If Not IsNumeric(sSelectedReportID) Then sSelectedReportID = CsvFirst(GetEngineReportIDsCSV(), "1")
sSelectedReportID = CStr(ToLongOrZero(sSelectedReportID))
If ToLongOrZero(sSelectedReportID) <= 0 Then sSelectedReportID = CsvFirst(GetEngineReportIDsCSV(), "1")
If InStr(1, sReportOptionsHTML, "value=""" & sSelectedReportID & """", vbTextCompare) = 0 Then sSelectedReportID = CsvFirst(EffectiveAllowedReportIDs, CsvFirst(GetEngineReportIDsCSV(), "1"))
If Not (EffectiveAllowedReportIDs = "ALL" Or CsvContainsInt(EffectiveAllowedReportIDs, sSelectedReportID)) Then
    sSelectedReportID = CsvFirst(EffectiveAllowedReportIDs, "0")
End If
sReportOptionsHTML = Replace(sReportOptionsHTML, "value=""" & sSelectedReportID & """", "value=""" & sSelectedReportID & """ selected", 1, 1, vbTextCompare)
Call ResolveReportMetadata()
If Len(Trim(ReportTitle)) = 0 Then ReportTitle = ReportName
SqlPopupUrl = GetReportSqlUrlByID(ReportID)
If Len(Trim(SqlPopupUrl)) = 0 Then SqlPopupUrl = "-"

EffectiveAllowedMinDate = FirstDayOfMonth(DateAdd("m", CLng(MasterDateStandardUserMinMonthOffset), Date()))
EffectiveAllowedMaxDate = LastDayOfMonth(DateAdd("m", CLng(MasterDateStandardUserMaxMonthOffset), Date()))
If AuthIsSuperAdmin Then
    EffectiveAllowedMinDate = ParseIsoDateOrDefault(MasterDateSuperAdminMinDateISO, DateSerial(2025, 10, 1))
    EffectiveAllowedMaxDate = LastDayOfMonth(DateAdd("m", CLng(MasterDateSuperAdminMaxMonthOffset), Date()))
End If
EffectiveAllowedMinDateISO = ToIsoDate(EffectiveAllowedMinDate)
EffectiveAllowedMaxDateISO = ToIsoDate(EffectiveAllowedMaxDate)
sStartDate = NormalizeIsoDateInRange(Request("start_date"), DateAdd("d", -ToLongOrZero(MasterEngineDefaultDateRangeDays), Date()), EffectiveAllowedMinDate, EffectiveAllowedMaxDate)
sEndDate = NormalizeIsoDateInRange(Request("end_date"), Date(), EffectiveAllowedMinDate, EffectiveAllowedMaxDate)
If ParseIsoDateOrDefault(sStartDate, Date()) > ParseIsoDateOrDefault(sEndDate, Date()) Then sStartDate = sEndDate

sSelectedClinic = Trim(Request("clinic"))
If Len(Trim(sSelectedClinic)) = 0 Then sSelectedClinic = "ALL"
sClinicHTML = "<option value=""ALL"">Todas las sucursales</option>"
sAllClinicIDs = CsvNormalizeIntList(EffectiveAllowedClinicIDs)
bSelectedClinicFound = (UCase(sSelectedClinic) = "ALL")
sSQL = "SELECT ClinicID, CONCAT(ClinicPrefix, ' - ', ClinicCommercialName) AS ClinicCommercialNameDisplay " & _
       "FROM x_config_clinics WHERE ClinicDisabled = 0 " & _
       "AND ClinicID IN (" & Gm_SqlSafeIntList(EffectiveAllowedClinicIDs) & ") " & _
       "ORDER BY FIELD(ClinicID," & Gm_SqlSafeIntList(MasterEngineCorporateClinicOrder) & ");"
Set rsClinics = Gm_OpenRs(sSQL)
If Not rsClinics Is Nothing Then
    Do While Not rsClinics.EOF
        cid = CStr(rsClinics("ClinicID"))
        cname = CStr(rsClinics("ClinicCommercialNameDisplay"))
        selectedAttr = ""
        If cid = sSelectedClinic Then
            selectedAttr = " selected"
            bSelectedClinicFound = True
        End If
        sClinicHTML = sClinicHTML & "<option value=""" & Server.HTMLEncode(cid) & """" & selectedAttr & ">" & Server.HTMLEncode(cname) & "</option>"
        rsClinics.MoveNext
    Loop
    If rsClinics.State = 1 Then rsClinics.Close
    Set rsClinics = Nothing
End If
If Not bSelectedClinicFound Then
    sSelectedClinic = "ALL"
    sClinicHTML = Replace(sClinicHTML, "<option value=""ALL"">", "<option value=""ALL"" selected>", 1, 1, vbTextCompare)
End If
If UCase(sSelectedClinic) = "ALL" Then
    CtxClinicIDs = sAllClinicIDs
Else
    CtxClinicIDs = CStr(ToLongOrZero(sSelectedClinic))
End If
CtxClinicIDs = CsvIntersect(CtxClinicIDs, EffectiveAllowedClinicIDs)
If Len(Trim(CtxClinicIDs)) = 0 Then CtxClinicIDs = CsvFirst(EffectiveAllowedClinicIDs, "0")

sReqFilter1IDs = GM_CsvFromRequest("filter1")
sReqFilter2IDs = GM_CsvFromRequest("filter2")
sReqFilter3IDs = GM_CsvFromRequest("filter3")
sFilter1IDs = sReqFilter1IDs : sFilter1ID = CsvFirst(sFilter1IDs, "0")
sFilter2IDs = sReqFilter2IDs : sFilter2ID = CsvFirst(sFilter2IDs, "0")
sFilter3IDs = sReqFilter3IDs : sFilter3ID = CsvFirst(sFilter3IDs, "0")

If Len(Trim(sSessionUip)) = 0 Then sSessionUip = GetServerVarText("REMOTE_ADDR")
sSessionUip = FirstIPToken(sSessionUip)
If Len(Trim(sSessionUip)) = 0 Then sSessionUip = "-"
sMetaIPMasked = MaskIPv4LastOctet(sSessionUip)

If Len(Trim(CtxTimeStamp)) = 0 Then CtxTimeStamp = ToIsoDateTime(Now())
CtxCSSpx = NormalizeCtxCssPx(Request("ctx_css_px"))
If Len(Trim(CtxCSSpx)) = 0 Then CtxCSSpx = "client-side"

bIsSqlPopupRequested = (Trim(CStr(Request("open_sql_popup"))) = "1")
bIsSqlPopupApiRequested = (Trim(CStr(Request("sql_popup_api"))) = "1")
bIsConfigPopupRequested = (Trim(CStr(Request("open_config_popup"))) = "1")
bIsConfigPopupApiRequested = (Trim(CStr(Request("config_popup_api"))) = "1")
sSqlPopupKind = LCase(Trim(CStr(Request("sql_popup_kind"))))
If sSqlPopupKind <> "filters" And sSqlPopupKind <> "kpis" And sSqlPopupKind <> "graph" Then sSqlPopupKind = "data"
sConfigPopupKind = LCase(Trim(CStr(Request("config_popup_kind"))))
If sConfigPopupKind = "engine" Then sConfigPopupKind = "reports"
If sConfigPopupKind <> "master" And sConfigPopupKind <> "modules" And sConfigPopupKind <> "home" And sConfigPopupKind <> "graph" Then sConfigPopupKind = "reports"
bSqlPopupAutoOpen = False
bConfigPopupAutoOpen = False
SqlPopupRaw = ""
SqlPopupClean = ""
SqlPopupContent = ""
SqlPopupStatusIcon = ""
SqlPopupBuildStatus = ""
SqlPopupBuildError = ""
ConfigPopupContent = ""
ConfigPopupStatusIcon = ""
ConfigPopupBuildStatus = ""
ConfigPopupBuildError = ""
If bIsConfigPopupRequested And bShowSqlCodeLink Then
    Dim configJsonTmp, configGraphTmp, configRemoteTmp
    configJsonTmp = ""
    configGraphTmp = ""
    configRemoteTmp = ""

    Select Case sConfigPopupKind
        Case "graph"
            ConfigPopupUrl = Trim(CStr(ReportGraphConfig))
            If Len(Trim(ConfigPopupUrl)) = 0 Then ConfigPopupUrl = "ReportGraphConfig"

            If ReportHasGraph Then
                configGraphTmp = Trim(CStr(GM_ResolveGraphConfigJson(ReportGraphConfig)))
                ConfigPopupContent = CStr(configGraphTmp)

                If Len(Trim(ConfigPopupContent)) = 0 Then
                    ConfigPopupBuildStatus = "EMPTY"
                    ConfigPopupStatusIcon = ""
                ElseIf Left(Trim(ConfigPopupContent), 1) = "{" Or Left(Trim(ConfigPopupContent), 1) = "[" Then
                    ConfigPopupBuildStatus = "OK"
                    ConfigPopupStatusIcon = "&#9989;"
                Else
                    ConfigPopupBuildStatus = "FAIL"
                    ConfigPopupBuildError = "GRAPH_CONFIG_JSON_INVALID_OR_EMPTY"
                    ConfigPopupStatusIcon = "&#10060;"
                End If
            Else
                ConfigPopupContent = Replace(CStr(MsgDevGraphNotConfigured), "{ReportName}", CStr(ReportName))
                ConfigPopupBuildStatus = "EMPTY"
                ConfigPopupStatusIcon = ""
            End If

        Case "master", "modules", "home"
            Select Case sConfigPopupKind
                Case "master"
                    ConfigPopupUrl = Trim(CStr(PathMasterConfigValue))
                Case "modules"
                    ConfigPopupUrl = Trim(CStr(PathModulesConfigValue))
                Case Else
                    ConfigPopupUrl = Trim(CStr(PathHomeConfigValue))
            End Select

            If Len(Trim(ConfigPopupUrl)) = 0 Then
                ConfigPopupContent = "Config URL no disponible."
                ConfigPopupBuildStatus = "EMPTY"
                ConfigPopupStatusIcon = ""
            Else
                If sConfigPopupKind = "master" And GM_IsValidMasterConfigJson(MasterConfigJsonText) Then
                    configRemoteTmp = Trim(CStr(MasterConfigJsonText))
                Else
                    configRemoteTmp = Trim(CStr(GM_LoadRemoteSql(ConfigPopupUrl)))
                End If
                ConfigPopupContent = CStr(configRemoteTmp)

                If Len(Trim(ConfigPopupContent)) = 0 Then
                    ConfigPopupBuildStatus = "EMPTY"
                    ConfigPopupStatusIcon = ""
                ElseIf Left(Trim(ConfigPopupContent), 1) = "{" Or Left(Trim(ConfigPopupContent), 1) = "[" Then
                    ConfigPopupBuildStatus = "OK"
                    ConfigPopupStatusIcon = "&#9989;"
                Else
                    ConfigPopupBuildStatus = "FAIL"
                    ConfigPopupBuildError = "CONFIG_JSON_INVALID_OR_EMPTY"
                    ConfigPopupStatusIcon = "&#10060;"
                End If
            End If

        Case Else
            ConfigPopupUrl = Trim(CStr(Report_Config_File))
            If Len(Trim(ConfigPopupUrl)) = 0 Then ConfigPopupUrl = "https://raw.githubusercontent.com/PedroTerlizzi/GM_BI_SKG/main/SKG_REPORT_CONFIG.json"

            On Error Resume Next
            configJsonTmp = Trim(CStr(Session("GM_Config_JSON")))
            If Err.Number <> 0 Then
                configJsonTmp = ""
                Err.Clear
            End If
            On Error GoTo 0

            ConfigPopupContent = CStr(configJsonTmp)

            If Len(Trim(ConfigPopupContent)) = 0 Then
                ConfigPopupBuildStatus = "EMPTY"
                ConfigPopupStatusIcon = ""
            ElseIf GM_IsValidReportsConfigJson(ConfigPopupContent) Then
                ConfigPopupBuildStatus = "OK"
                ConfigPopupStatusIcon = "&#9989;"
            Else
                ConfigPopupBuildStatus = "FAIL"
                ConfigPopupBuildError = "CONFIG_JSON_INVALID_OR_EMPTY"
                ConfigPopupStatusIcon = "&#10060;"
            End If
    End Select

    bConfigPopupAutoOpen = True
End If

If bIsConfigPopupApiRequested Then
    Response.Clear
    Response.Buffer = True
    Response.Charset = "windows-1252"
    Response.ContentType = "application/json"

    If bShowSqlCodeLink And bIsConfigPopupRequested Then
        Response.Write "{""ok"":true,""buildStatus"":""" & GmJsonEscape(ConfigPopupBuildStatus) & """,""buildError"":""" & GmJsonEscape(ConfigPopupBuildError) & """,""url"":""" & GmJsonEscape(ConfigPopupUrl) & """,""config"":""" & GmJsonEscape(ConfigPopupContent) & """}"
    ElseIf Not bShowSqlCodeLink Then
        Response.Status = "403 Forbidden"
        Response.Write "{""ok"":false,""error"":""CONFIG_POPUP_NOT_ALLOWED""}"
    Else
        Response.Status = "400 Bad Request"
        Response.Write "{""ok"":false,""error"":""CONFIG_POPUP_BAD_REQUEST""}"
    End If

    Response.End
End If

If bIsSqlPopupRequested And bShowSqlCodeLink Then
    Dim scopeTmp, sqlInjected, buildStatus, buildError, sqlMissingMsg
    sqlMissingMsg = ""
    Set scopeTmp = BuildScopeFull()

    Select Case sSqlPopupKind
        Case "filters"
            SqlPopupUrl = GetReportFiltersSqlUrlByID(ReportID)
            If Not ReportHasFilters Then sqlMissingMsg = Replace(CStr(MsgDevFiltersNotConfigured), "{ReportName}", CStr(ReportName))
            If Len(Trim(SqlPopupUrl)) = 0 And Len(Trim(sqlMissingMsg)) = 0 Then sqlMissingMsg = Replace(CStr(MsgDevFiltersNotConfigured), "{ReportName}", CStr(ReportName))
        Case "kpis"
            SqlPopupUrl = GetReportKPIsSqlUrlByID(ReportID)
            If Not ReportHasKPIs Then sqlMissingMsg = Replace(CStr(MsgDevKpisNotConfigured), "{ReportName}", CStr(ReportName))
            If Len(Trim(SqlPopupUrl)) = 0 And Len(Trim(sqlMissingMsg)) = 0 Then sqlMissingMsg = Replace(CStr(MsgDevKpisNotConfigured), "{ReportName}", CStr(ReportName))
        Case "graph"
            SqlPopupUrl = GetReportGraphSqlUrlByID(ReportID)
            If Not ReportHasGraph Then sqlMissingMsg = Replace(CStr(MsgDevGraphNotConfigured), "{ReportName}", CStr(ReportName))
            If Len(Trim(SqlPopupUrl)) = 0 And Len(Trim(sqlMissingMsg)) = 0 Then sqlMissingMsg = Replace(CStr(MsgDevGraphNotConfigured), "{ReportName}", CStr(ReportName))
        Case Else
            SqlPopupUrl = GetReportSqlUrlByID(ReportID)
            If Len(Trim(SqlPopupUrl)) = 0 Then sqlMissingMsg = Replace(CStr(MsgDevDataSqlNotConfigured), "{ReportName}", CStr(ReportName))
    End Select

    If Len(Trim(SqlPopupUrl)) = 0 Then SqlPopupUrl = "-"

    If Len(Trim(sqlMissingMsg)) > 0 Then
        SqlPopupBuildStatus = "EMPTY"
        SqlPopupBuildError = "SQL_NOT_CONFIGURED"
        SqlPopupStatusIcon = ""
        SqlPopupContent = sqlMissingMsg
    Else
        buildStatus = "" : buildError = ""
        sqlInjected = GM_BuildExecutableSqlByUrl(SqlPopupUrl, scopeTmp, buildStatus, buildError)
        SqlPopupBuildStatus = buildStatus
        SqlPopupBuildError = buildError

        If buildStatus = "OK" Then
            SqlPopupStatusIcon = "&#9989;"
        Else
            SqlPopupStatusIcon = "&#10060;"
        End If

        SqlPopupContent = Trim(sqlInjected)

        If Len(Trim(SqlPopupContent)) = 0 Then
            SqlPopupContent = CStr(MsgDevSqlEmptyRender)
        End If
    End If

    Set scopeTmp = Nothing
    bSqlPopupAutoOpen = True
End If

If bIsSqlPopupApiRequested Then
    Response.Clear
    Response.Buffer = True
    Response.Charset = "windows-1252"
    Response.ContentType = "application/json"

    If bShowSqlCodeLink And bIsSqlPopupRequested Then
        Response.Write "{""ok"":true,""isSafe"":" & LCase(CStr(SqlPopupBuildStatus = "OK")) & _
                       ",""buildStatus"":""" & GmJsonEscape(SqlPopupBuildStatus) & """,""buildError"":""" & GmJsonEscape(SqlPopupBuildError) & """,""url"":""" & GmJsonEscape(SqlPopupUrl) & """,""sql"":""" & GmJsonEscape(SqlPopupContent) & """}"
    ElseIf Not bShowSqlCodeLink Then
        Response.Status = "403 Forbidden"
        Response.Write "{""ok"":false,""error"":""SQL_POPUP_NOT_ALLOWED""}"
    Else
        Response.Status = "400 Bad Request"
        Response.Write "{""ok"":false,""error"":""SQL_POPUP_BAD_REQUEST""}"
    End If

    Response.End
End If

bIsExportRequested = (LCase(Trim(Request("action"))) = "export")
bIsDevOnlyPopupFlow = ((bIsSqlPopupRequested Or bIsConfigPopupRequested) And bShowSqlCodeLink)
If bIsExportRequested Then
    Call Gm_ExportReport()
ElseIf bIsDevOnlyPopupFlow Then
    Set rsData = Nothing
    Set rsKPIs = Nothing
    Set rsGraph = Nothing
Else
    Call Gm_ExecuteReport()
End If

If ReportHasGraph Then
    GraphDataJson = GM_RecordsetToJsonArray(rsGraph)
    GraphConfigJson = GM_ResolveGraphConfigJson(ReportGraphConfig)
Else
    GraphDataJson = "[]"
    GraphConfigJson = GM_GetDefaultGraphConfigJson()
End If

Call Gm_BuildTxtMetadatos()
TxtLogExecutionTimestamp = Trim(CStr(GM_JsonGetString(TxtWebhookPayload, "LogExecutionTimestamp")))
If Len(Trim(TxtLogExecutionTimestamp)) = 0 Then TxtLogExecutionTimestamp = Trim(CStr(CtxTimeStamp))
If Len(Trim(TxtLogExecutionTimestamp)) = 0 Then TxtLogExecutionTimestamp = "-"
sWaOperacionesMsg = Replace(CStr(WAOpsTemplateValue), "{ReportTitle}", CStr(ReportTitle))
sWaSoporteMsg = Replace(CStr(WASupportTemplateValue), "{MetadataText}", CStr(TxtMetadatos))
sWaOperacionesMsg = GmAsciiSafe(sWaOperacionesMsg)
sWaSoporteMsg = GmAsciiSafe(sWaSoporteMsg)
sWaOperacionesUrl = BuildWaUrl(OperationsWA, sWaOperacionesMsg)
sWaSoporteUrl = BuildWaUrl(SupportWA, sWaSoporteMsg)
%>

<!-- ------------------------------------------------------------------------------
UI LAYOUT
------------------------------------------------------------------------------ -->
<!DOCTYPE html>
<html>
<head>
    <meta charset="windows-1252">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Growmetrica Control Center</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;700&display=swap" rel="stylesheet">
    <!-- ------------------------------------------------------------------------------
CSS STYLES
------------------------------------------------------------------------------ -->
    <style>
        
        :root {
            --gm-bg: #f1f1f5;
            --gm-card: #ffffff;
            --gm-stroke: #e5e7e9;
            --gm-soft-stroke: #eceff2;
            --gm-text: #111827;
            --gm-muted: #6b7280;
            --gm-primary: #350072;
            --gm-primary-soft: rgba(53, 0, 114, 0.12);
        }

        html, body {
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden;
            background: var(--gm-bg);
            color: var(--gm-text);
        }
        body.gm-sql-modal-open {
            overflow: hidden;
        }

        #co-loader-overlay {
            position: fixed;
            inset: 0;
            z-index: 2147483647;
            display: none;
            align-items: center;
            justify-content: center;
            background: transparent;
            -webkit-backdrop-filter: blur(6px);
            backdrop-filter: blur(6px);
        }

        #co-loader-card {
            width: 320px;
            padding: 22px;
            background: transparent;
            border-radius: 14px;
            box-shadow: none;
            text-align: center;
            font-family: "DM Sans", "Open Sans", Arial, sans-serif;
        }

        #co-loader-title {
            font-size: 16px;
            font-weight: 700;
            color: #111827;
            margin: 0 0 8px 0;
            line-height: 1.2;
        }

        #co-loader-box {
            position: relative;
            width: 56px;
            height: 56px;
            margin: 0 auto 8px auto;
        }

        #co-loader-logo {
            width: 36px;
            height: 36px;
            border-radius: 50%;
            object-fit: contain;
            background: transparent;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            z-index: 2;
        }

        #co-loader-ring {
            position: absolute;
            inset: 0;
            border-radius: 50%;
            border: 3px solid #d1d5db;
            border-top-color: #111827;
            border-right-color: #374151;
            animation: co-spin 1s linear infinite;
        }

        #co-loader-msg {
            font-family: "DM Sans", "Open Sans", Arial, sans-serif;
            font-size: 12px;
            line-height: 1.2;
            color: #6b7280;
            margin-top: 6px;
            opacity: 1;
        }

        #co-loader-progress {
            margin-top: 10px;
            height: 8px;
            background: #eee;
            border-radius: 8px;
            overflow: hidden;
        }

        #co-loader-bar {
            height: 100%;
            width: 0%;
            background: #111827;
            transition: width 0.35s ease;
        }

        @supports not ((-webkit-backdrop-filter: blur(1px)) or (backdrop-filter: blur(1px))) {
            #co-loader-overlay {
                background: #f1f1f5;
            }
        }

        @keyframes co-spin {
            to {
                transform: rotate(360deg);
            }
        }

        .co-lock-scroll {
            overflow: hidden !important;
        }

        .gm-report,
        .gm-report * {
            font-family: "DM Sans", "Open Sans", Arial, sans-serif !important;
            box-sizing: border-box;
        }

        .gm-report {
            --gm-air: 16px;
            --gm-air-100: var(--gm-air);
            --gm-air-050: calc(var(--gm-air) * 0.5);
            --gm-air-025: calc(var(--gm-air) * 0.25);
            --gm-air-0125: calc(var(--gm-air) * 0.125);
            --gm-air-075: calc(var(--gm-air) * 0.75);
            --gm-action-btn-width: 130px;
            --gm-action-btn-height: 46px;
            --gm-action-offset-x: 24px;
            --gm-kpi-height: 62px;
            width: 100%;
            height: 100dvh;
            padding: var(--gm-air-025) 12px 10px 12px;
            display: grid;
            grid-template-rows: auto auto minmax(0, 1fr) auto;
            gap: 0;
            overflow: hidden;
            background: var(--gm-bg);
        }

        .topbar {
            display: grid;
            grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
            align-items: center;
            gap: 12px;
            padding: 0 2px;
            min-height: 0;
        }

        .gm-meta,
        .topbar-left {
            font-size: 12px;
            color: #6b7280;
            line-height: 1.12;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .topbar-left {
            justify-self: start;
            min-width: 0;
        }

        .topbar-right {
            font-size: 12px;
            color: #6b7280;
            line-height: 1.12;
            white-space: nowrap;
            overflow: visible;
            text-overflow: ellipsis;
            justify-self: end;
            text-align: right;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 8px;
        }

        .gm-topbar-meta-popwrap {
            display: inline-block;
            flex: 0 0 auto;
        }

        .gm-topbar-center {
            justify-self: center;
            min-width: 0;
        }

        .gm-topbar-userform {
            margin: 0;
            line-height: 1;
        }

        .gm-topbar-user-inline {
            display: flex;
            align-items: center;
            gap: 6px;
            min-width: 0;
        }

        .gm-topbar-user-label {
            font-size: 12px;
            color: #6b7280;
            line-height: 1.12;
            white-space: nowrap;
        }

        .gm-topbar-user-wrapper {
            position: relative;
            display: inline-block;
            max-width: clamp(250px, 36vw, 480px);
            font-size: 12px;
            color: #2b2b2b;
            vertical-align: baseline;
            min-width: 0;
        }

        .gm-topbar-user-trigger {
            appearance: none;
            -webkit-appearance: none;
            -webkit-tap-highlight-color: transparent;
            border: 0;
            background: transparent;
            color: #2b2b2b;
            font: inherit;
            line-height: 1.12;
            margin: 0;
            padding: 0 12px 1px 0;
            width: 100%;
            text-align: left;
            border-bottom: 1px solid rgba(43, 43, 43, 0.25);
            cursor: pointer;
            position: relative;
            user-select: none;
            -webkit-user-select: none;
            -moz-user-select: none;
            -ms-user-select: none;
        }

        .gm-topbar-user-trigger:hover,
        .gm-topbar-user-trigger:focus-visible {
            border-bottom-color: rgba(43, 43, 43, 0.45);
            outline: none;
        }

        .gm-topbar-user-text {
            display: block;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            user-select: none;
            -webkit-user-select: none;
            -moz-user-select: none;
            -ms-user-select: none;
        }

        .gm-topbar-user-trigger::selection,
        .gm-topbar-user-text::selection {
            background: transparent;
        }

        .gm-topbar-user-trigger::-moz-selection,
        .gm-topbar-user-text::-moz-selection {
            background: transparent;
        }

        
        .gm-topbar-user-trigger::after {
            content: "";
            position: absolute;
            right: 0;
            top: 50%;
            width: 9px;
            height: 9px;
            transform: translateY(-50%);
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 12'%3E%3Cpath fill='%232b2b2b' d='M3 5l3 3 3-3z'/%3E%3C/svg%3E");
            background-size: contain;
            background-repeat: no-repeat;
        }

        .gm-topbar-user-wrapper.is-open .gm-topbar-user-trigger::after {
            transform: translateY(-50%) rotate(180deg);
        }

        .gm-topbar-user-menu {
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            min-width: 260px;
            width: max-content;
            max-width: min(520px, calc(100vw - 24px));
            max-height: min(320px, 60vh);
            overflow-y: auto;
            overflow-x: hidden;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 8px 24px rgba(17, 24, 39, 0.14);
            z-index: 120;
            display: none;
        }

        .gm-topbar-user-wrapper.is-open .gm-topbar-user-menu {
            display: block;
        }

        .gm-topbar-user-option {
            display: block;
            width: 100%;
            border: 0;
            background: transparent;
            color: #2b2b2b;
            font-family: "DM Sans", "Open Sans", Arial, sans-serif;
            font-size: 12px;
            line-height: 1.2;
            text-align: left;
            padding: 8px 10px;
            cursor: pointer;
            white-space: nowrap;
        }

        .gm-topbar-user-option:hover {
            background: #f3f4f6;
        }

        .gm-topbar-user-option.is-selected {
            background: var(--gm-primary-soft);
            color: #111827;
            font-weight: 500;
        }

        
        .gm-topbar-user-wrapper select {
            position: absolute;
            left: 0;
            top: 0;
            width: 1px;
            height: 1px;
            opacity: 0;
            pointer-events: none;
        }

        .topbar-right a,
        .topbar-right a:visited {
            color: #2b2b2b;
            text-decoration: none !important;
            border-bottom: 1px solid rgba(43, 43, 43, 0.25) !important;
            margin-left: 0;
            display: inline-block;
            line-height: 1.2;
            padding-bottom: 1px;
        }

        .gm-modal {
            border: 1px solid var(--gm-stroke);
            border-radius: 12px;
            background: var(--gm-card);
            overflow: hidden;
            min-width: 0;
        }

        .menu.gm-modal,
        .main.gm-modal {
            border-color: transparent;
        }

        .menu.gm-modal {
            overflow: visible;
        }

        .gm-sql-modal {
            position: fixed;
            top: 0;
            right: 0;
            bottom: 0;
            left: 0;
            display: none;
            align-items: center;
            justify-content: center;
            background: rgba(241, 241, 245, 0.82);
            -webkit-backdrop-filter: blur(6px);
            backdrop-filter: blur(6px);
            z-index: 9999;
            padding: 18px;
        }

        @supports not ((-webkit-backdrop-filter: blur(1px)) or (backdrop-filter: blur(1px))) {
            .gm-sql-modal {
                background: #f1f1f5;
            }
        }

        .gm-sql-modal.is-open {
            display: flex;
        }

        .gm-sql-modal-dialog {
            width: min(1280px, 96vw);
            height: min(92vh, 960px);
            background: #ffffff;
            border-radius: 12px;
            border: 1px solid var(--gm-soft-stroke);
            box-shadow: 0 18px 46px rgba(17, 24, 39, 0.35);
            display: grid;
            grid-template-rows: auto minmax(0, 1fr);
            min-width: 0;
        }

        .gm-sql-modal-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            padding: 12px 14px;
            border-bottom: 1px solid var(--gm-soft-stroke);
            background: #f8fafc;
            border-top-left-radius: 12px;
            border-top-right-radius: 12px;
        }

        .gm-sql-modal-title {
            font-size: 13px;
            font-weight: 600;
            color: #111827;
            line-height: 1.2;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .gm-sql-modal-actions {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .gm-sql-modal-copy,
        .gm-sql-modal-close {
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
            color: #111827;
            font-size: 12px;
            line-height: 1;
            padding: 8px 10px;
            cursor: pointer;
        }

        .gm-sql-modal-copy:hover,
        .gm-sql-modal-copy:focus-visible,
        .gm-sql-modal-close:hover,
        .gm-sql-modal-close:focus-visible {
            background: #f3f4f6;
            outline: none;
        }

        .gm-sql-modal-body {
            min-height: 0;
            overflow: auto;
            padding: 12px 14px 14px 14px;
            background: #ffffff;
            border-bottom-left-radius: 12px;
            border-bottom-right-radius: 12px;
        }

        .gm-sql-modal-code {
            margin: 0;
            min-height: 100%;
            white-space: pre;
            color: #111827;
            font-size: 12px;
            line-height: 1.4;
            font-family: Consolas, "Courier New", monospace !important;
        }

        .gm-config-modal-body {
            overflow: hidden;
            display: flex;
            align-items: stretch;
            justify-content: stretch;
        }

        .gm-config-table-wrap {
            width: 100%;
            min-height: 0;
            height: 100%;
            overflow: auto;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
        }

        .gm-config-table-host {
            min-width: 100%;
            min-height: 100%;
        }

        .gm-config-table {
            width: max-content;
            min-width: 100%;
            border-collapse: collapse;
            background: #ffffff;
            font-size: 12px;
            line-height: 1.25;
        }

        .gm-config-table th,
        .gm-config-table td {
            padding: 8px 10px;
            border-bottom: 1px solid var(--gm-soft-stroke);
            border-right: 1px solid var(--gm-soft-stroke);
            text-align: left;
            vertical-align: top;
            white-space: nowrap;
            max-width: 420px;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .gm-config-table th:last-child,
        .gm-config-table td:last-child {
            border-right: 0;
        }

        .gm-config-table thead th {
            position: sticky;
            top: 0;
            z-index: 5;
            background: #f7f8fb;
            color: #111827;
            font-weight: 700;
        }

        .gm-config-freeze-col {
            position: sticky;
            background: #ffffff;
            z-index: 2;
            box-shadow: 1px 0 0 var(--gm-soft-stroke);
        }

        .gm-config-table thead .gm-config-freeze-col {
            z-index: 8;
            background: #eef2ff;
        }

        .gm-config-modal-fallback {
            margin: 0;
            min-height: 100%;
            white-space: pre-wrap;
            color: #111827;
            font-size: 12px;
            line-height: 1.4;
            font-family: Consolas, "Courier New", monospace !important;
            padding: 10px;
        }

        
        .menu {
            padding: 24px 16px 16px 16px;
            margin-top: var(--gm-air-050);
        }

        .menu .menu-grid {
            display: grid;
            grid-template-columns:
                2fr
                2fr
                1fr
                1fr
                var(--gm-action-btn-width);
            column-gap: 16px;
            align-items: stretch;
            padding: 0 16px;
        }

        .menu .gm-field-date:first-of-type {
            margin-left: 24px;
        }

        .menu .menu-grid > .gm-field {
            position: relative;
            min-width: 0;
            min-height: 44px;
            display: flex;
            flex-direction: column;
            justify-content: flex-end;
            align-items: stretch;
        }

        .menu .gm-field-action {
            margin-left: var(--gm-action-offset-x);
        }

        .menu .gm-field-date {
            min-width: 0;
        }

        .menu .menu-grid > .gm-field > .gm-control,
        .menu .menu-grid > .gm-field > .gm-btn {
            height: 44px;
            margin: 0;
        }

        .menu .menu-grid > .gm-field > .gm-btn {
            height: var(--gm-action-btn-height);
            width: 100%;
        }

        .menu .gm-selectbox {
            position: relative;
            width: 100%;
            min-height: 44px;
        }

        .menu .gm-select-trigger {
            width: 100%;
            height: 44px;
            border: 1px solid var(--gm-primary);
            border-radius: 11px;
            padding: 0 34px 0 14px;
            background: #ffffff;
            color: var(--gm-text);
            text-align: left;
            font-size: 14px;
            font-family: "DM Sans", sans-serif;
            line-height: 1.2;
            cursor: pointer;
            position: relative;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .menu .gm-select-trigger:focus-visible {
            outline: none;
            box-shadow: 0 0 0 3px rgba(53,0,114,0.15);
        }

        .menu .gm-select-trigger::after {
            content: "";
            position: absolute;
            right: 12px;
            top: 50%;
            width: 10px;
            height: 10px;
            transform: translateY(-50%);
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 12'%3E%3Cpath fill='%23350072' d='M3 5l3 3 3-3z'/%3E%3C/svg%3E");
            background-size: contain;
            background-repeat: no-repeat;
        }

        .menu .gm-selectbox.is-open .gm-select-trigger::after {
            transform: translateY(-50%) rotate(180deg);
        }

        .menu .gm-select-text {
            display: block;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .menu .gm-select-menu {
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            right: 0;
            max-height: min(280px, 55vh);
            overflow-y: auto;
            overflow-x: hidden;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 8px 24px rgba(17, 24, 39, 0.14);
            z-index: 130;
            display: none;
        }

        .menu .gm-selectbox.is-open .gm-select-menu {
            display: block;
        }

        .menu .gm-select-option {
            display: block;
            width: 100%;
            border: 0;
            background: transparent;
            color: #2b2b2b;
            font-family: "DM Sans", "Open Sans", Arial, sans-serif;
            font-size: 13px;
            line-height: 1.25;
            text-align: left;
            padding: 8px 10px;
            cursor: pointer;
            white-space: nowrap;
        }

        .menu .gm-select-option:hover {
            background: #f3f4f6;
        }

        .menu .gm-select-option.is-selected {
            background: var(--gm-primary-soft);
            color: #111827;
            font-weight: 500;
        }

        .menu .gm-selectbox select {
            position: absolute;
            left: 0;
            top: 0;
            width: 1px;
            height: 1px;
            opacity: 0;
            pointer-events: none;
        }

        .gm-label {
            position: absolute;
            top: -9px;
            left: 12px;
            padding: 0 6px;
            font-size: 12px;
            color: var(--gm-primary);
            background: var(--gm-card);
            z-index: 2;
            pointer-events: none;
        }

        .gm-control {
            width: 100%;
            height: 44px;
            border: 1px solid var(--gm-primary);
            border-radius: 11px;
            padding: 0 14px;
            background: #ffffff;
            color: var(--gm-text);
            outline: none;
            font-size: 14px;
            font-family: "DM Sans", sans-serif;
            line-height: 1.2;
        }

        .gm-control[type="date"] {
            border: 1px solid var(--gm-primary) !important;
            border-radius: 11px !important;
            background-color: #ffffff !important;
            height: 44px;
            padding-top: 0;
            padding-bottom: 0;
            padding-right: 40px;
            position: relative;
        }

        .gm-report input[type="date"] {
            -webkit-appearance: none;
            appearance: none;
        }

        .gm-control[type="date"]::-webkit-calendar-picker-indicator {
            position: absolute;
            right: 14px;
            cursor: pointer;
        }

        .gm-control[type="date"]::-webkit-datetime-edit {
            display: flex;
            align-items: center;
            height: 44px;
            padding-left: 10px;
        }

        .gm-control:focus {
            box-shadow: 0 0 0 3px rgba(53,0,114,0.15);
        }

        .gm-btn {
            height: 46px;
            border-radius: 11px;
            border: 1px solid #350072;
            padding: 0 20px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            font-family: "DM Sans", sans-serif !important;
            background: #350072;
            color: #ffffff;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .menu .gm-btn {
            -webkit-appearance: none;
            appearance: none;
            line-height: 1;
        }

        
        .main {
            min-height: 0;
            display: flex;
            flex-direction: column;
            background: #ffffff;
            margin: var(--gm-air) 0 0 0;
            padding: 0 !important;
            overflow: hidden;
        }

        .main-content {
            flex: 1 1 auto;
            min-height: 0;
            margin: 0 !important;
            overflow-y: auto;
            overflow-x: hidden;
            padding: var(--gm-air) 16px 16px;
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 20px;
            align-content: start;
            justify-content: start;
            align-items: start;
            background: #ffffff;
        }

        .main-content.is-report1 {
            --gm-main-table-scroll-gap: 16px;
            display: flex;
            flex-direction: column;
            align-items: stretch;
            justify-content: flex-start;
            gap: var(--gm-air-075);
            overflow: hidden;
            padding-top: var(--gm-air-100);
        }

        .main-content > * {
            min-width: 0;
        }

        .main-content .gm-main-panel {
            margin: 0;
        }

        .gm-main-report-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            min-width: 0;
            padding: 0;
        }

        .gm-main-report-title {
            flex: 0 0 auto;
            font-size: 12px;
            line-height: 1.12;
            font-weight: 700;
            color: #111827;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .gm-main-report-filters {
            min-width: 0;
            text-align: right;
            margin-right: calc(var(--gm-air-100) + var(--gm-air-050));
        }

        .gm-main-report-filter-form {
            --gm-filter-dd-width: 220px;
            --gm-filter-dd-height: 25px;
            margin: 0;
            display: inline-flex;
            align-items: stretch;
            justify-content: flex-end;
            gap: 10px;
            flex-wrap: wrap;
            min-width: 0;
            max-width: 100%;
        }

        .gm-main-report-filter-group {
            position: relative;
            display: inline-flex;
            align-items: stretch;
            width: var(--gm-filter-dd-width);
            min-width: var(--gm-filter-dd-width);
            max-width: var(--gm-filter-dd-width);
            min-height: var(--gm-filter-dd-height);
            background: #f1f1f5;
            border-radius: 10px;
        }

        .gm-main-report-filter-label {
            display: none;
        }

        .gm-main-report-filter-selectbox {
            position: relative;
            width: 100%;
            min-height: var(--gm-filter-dd-height);
        }

        .gm-main-report-filter-selectbox .gm-select-trigger {
            width: 100%;
            height: var(--gm-filter-dd-height);
            border: 0;
            border-radius: 10px;
            padding: 0 28px 0 10px;
            background: #f1f1f5;
            color: #6b7280;
            text-align: left;
            font-size: 13px;
            font-family: "DM Sans", sans-serif;
            line-height: 1.2;
            cursor: pointer;
            position: relative;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .gm-main-report-filter-selectbox .gm-select-trigger:focus-visible {
            outline: none;
            box-shadow: 0 0 0 2px rgba(53,0,114,0.1);
        }

        .gm-main-report-filter-selectbox .gm-select-trigger::after {
            content: "";
            position: absolute;
            right: 9px;
            top: 50%;
            width: 8px;
            height: 8px;
            transform: translateY(-50%);
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 12'%3E%3Cpath fill='%23350072' d='M3 5l3 3 3-3z'/%3E%3C/svg%3E");
            background-size: contain;
            background-repeat: no-repeat;
        }

        .gm-main-report-filter-selectbox.is-open .gm-select-trigger::after {
            transform: translateY(-50%) rotate(180deg);
        }

        .gm-main-report-filter-selectbox .gm-select-text {
            display: block;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .gm-main-report-filter-selectbox .gm-select-menu {
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            right: 0;
            max-height: min(280px, 55vh);
            overflow-y: auto;
            overflow-x: hidden;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 8px 24px rgba(17, 24, 39, 0.14);
            z-index: 130;
            display: none;
        }

        .gm-main-report-filter-selectbox.is-open .gm-select-menu {
            display: block;
        }

        .gm-main-report-filter-selectbox .gm-select-option {
            display: block;
            width: 100%;
            border: 0;
            background: transparent;
            color: #2b2b2b;
            font-family: "DM Sans", "Open Sans", Arial, sans-serif;
            font-size: 13px;
            line-height: 1.25;
            text-align: left;
            padding: 8px 10px;
            cursor: pointer;
            white-space: nowrap;
        }

        .gm-main-report-filter-selectbox .gm-select-option:hover {
            background: #f3f4f6;
        }

        .gm-main-report-filter-selectbox .gm-select-option.is-selected {
            background: var(--gm-primary-soft);
            color: #111827;
            font-weight: 500;
        }

        .gm-main-report-filter-control {
            position: absolute;
            left: 0;
            top: 0;
            width: 1px;
            height: 1px;
            opacity: 0;
            pointer-events: none;
        }

        .gm-main-layout {
            flex: 1 1 auto;
            min-height: 0;
            display: block;
        }

        .main-content.is-report1 #LayoutMain-Container {
            box-sizing: border-box;
            width: 100%;
            min-width: 0;
        }

        .main-content.is-report1 #LayoutMain-Container:not(.has-graph) {
            min-height: 0;
            height: 100%;
            display: grid;
            grid-template-columns: minmax(0, 1fr);
            align-items: stretch;
        }

        .main-content.is-report1 #CmpMainTable {
            width: 100%;
            max-width: 100%;
            min-width: 0;
            margin-right: 0;
            padding-right: 0;
            justify-self: stretch;
            align-self: stretch;
        }

        .gm-main-layout.has-graph {
            display: grid;
            grid-template-columns: minmax(0, 3fr) minmax(0, 7fr);
            gap: 12px;
            align-items: stretch;
        }

        .gm-main-layout.has-graph > * {
            min-width: 0;
        }

        .gm-main-panel {
            padding: 14px;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 11px;
            background: #ffffff;
        }

        .gm-main-panel-title {
            font-size: 13px;
            font-weight: 700;
            color: #111827;
            margin-bottom: 10px;
        }

        .gm-main-list {
            display: grid;
            gap: 10px;
        }

        .gm-main-item {
            padding: 8px 10px;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
            font-size: 13px;
            line-height: 1.25;
            word-break: break-word;
        }

        .gm-main-table-panel {
            grid-column: 1 / -1;
        }

        .gm-main-table-shell {
            flex: 1 1 auto;
            min-height: 0;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 11px;
            background: #ffffff;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }

        .gm-main-table-wrap {
            width: 100%;
            overflow-x: auto;
            overflow-y: auto;
            max-height: 100%;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            box-sizing: border-box;
            scrollbar-color: #f1f1f5 transparent;
            scrollbar-width: thin;
        }

        .gm-main-table-wrap::-webkit-scrollbar,
        .gm-main-table-vscroll::-webkit-scrollbar {
            width: 12px;
            height: 12px;
            background: transparent;
        }

        .gm-main-table-wrap::-webkit-scrollbar-track,
        .gm-main-table-vscroll::-webkit-scrollbar-track {
            background: transparent;
        }

        .gm-main-table-wrap::-webkit-scrollbar-thumb,
        .gm-main-table-vscroll::-webkit-scrollbar-thumb {
            background-color: #f1f1f5;
            border-radius: 999px;
            border: 0;
            min-height: 24px;
            min-width: 24px;
        }

        .gm-main-table-wrap::-webkit-scrollbar-corner,
        .gm-main-table-vscroll::-webkit-scrollbar-corner {
            background: transparent;
        }

        .main-content.is-report1 .gm-main-table-wrap {
            flex: 1 1 auto;
            min-height: 0;
            height: 100%;
            max-height: none;
            border: 0;
            border-radius: 0;
        }

        .gm-main-table-vscroll {
            width: max-content;
            min-width: 100%;
            height: auto;
            min-height: 0;
            overflow-y: visible;
            overflow-x: visible;
            box-sizing: border-box;
            scrollbar-color: #f1f1f5 transparent;
            scrollbar-width: thin;
        }

        .main-content.is-report1 .gm-main-table-vscroll {
            min-height: 100%;
        }

        .gm-main-table-wrap.gm-no-hscroll {
            overflow-x: hidden;
        }

        .gm-main-table-wrap.gm-no-hscroll .gm-main-table {
            width: 100% !important;
            min-width: 100% !important;
            max-width: 100%;
            table-layout: fixed;
        }

        .gm-main-table-wrap.gm-no-hscroll .gm-main-table th,
        .gm-main-table-wrap.gm-no-hscroll .gm-main-table td {
            white-space: normal;
            word-break: break-word;
            overflow-wrap: anywhere;
            max-width: 1px;
        }

        .gm-main-table-wrap.gm-no-hscroll .gm-main-table-sort {
            white-space: normal;
            overflow-wrap: anywhere;
        }

        .gm-main-table-wrap .gm-main-table th:first-child,
        .gm-main-table-wrap .gm-main-table td:first-child {
            position: sticky;
            left: 0;
            z-index: 2;
            background: #ffffff;
            box-shadow: 1px 0 0 var(--gm-soft-stroke);
        }

        .gm-main-table-wrap .gm-main-table thead th:first-child {
            z-index: 5;
            background: #f7f8fb;
        }

        .gm-main-table-wrap .gm-main-table tbody tr:nth-child(even) td:first-child {
            background: #fbfcff;
        }

        .gm-main-table {
            width: 100%;
            min-width: 1040px;
            border-collapse: collapse;
            background: #ffffff;
            font-size: 12px;
            line-height: 1.25;
        }

        .gm-main-table th,
        .gm-main-table td {
            padding: 8px 10px;
            border-bottom: 1px solid var(--gm-soft-stroke);
            border-right: 1px solid var(--gm-soft-stroke);
            text-align: left;
            vertical-align: middle;
            white-space: nowrap;
        }

        .gm-main-table th:last-child,
        .gm-main-table td:last-child {
            border-right: 0;
        }

        .gm-main-table thead th {
            position: sticky;
            top: 0;
            z-index: 3;
            background: #f7f8fb;
            color: #111827;
            font-weight: 700;
            user-select: none !important;
            -webkit-user-select: none !important;
            -moz-user-select: none !important;
            -ms-user-select: none !important;
        }

        .gm-main-table thead th * {
            user-select: none !important;
            -webkit-user-select: none !important;
            -moz-user-select: none !important;
            -ms-user-select: none !important;
        }

        .gm-main-table-sort {
            width: 100%;
            border: 0;
            background: transparent;
            color: inherit;
            font: inherit;
            font-weight: 700;
            text-align: left;
            padding: 0;
            margin: 0;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            cursor: pointer;
            user-select: none;
            -webkit-user-select: none;
            -moz-user-select: none;
            -ms-user-select: none;
            -webkit-appearance: none;
            appearance: none;
            -webkit-tap-highlight-color: transparent;
        }

        .gm-main-table-sort:hover,
        .gm-main-table-sort:active,
        .gm-main-table-sort:focus,
        .gm-main-table-sort:focus-visible {
            background: transparent !important;
            color: inherit;
            outline: none;
            box-shadow: none;
        }

        .gm-main-table-sort::selection,
        .gm-main-table-sort *::selection,
        .gm-main-table thead th::selection {
            background: transparent !important;
            color: inherit;
        }

        .gm-main-table-sort::-moz-selection,
        .gm-main-table-sort *::-moz-selection,
        .gm-main-table thead th::-moz-selection {
            background: transparent !important;
            color: inherit;
        }

        .gm-main-table-sort::after {
            content: "";
            width: 0;
            height: 0;
            border-left: 4px solid transparent;
            border-right: 4px solid transparent;
            border-top: 5px solid #9ca3af;
            opacity: 0.85;
            transform: translateY(1px);
            transition: border-color 0.12s ease, opacity 0.12s ease;
        }

        .gm-main-table-sort.is-asc::after {
            border-top: 0;
            border-bottom: 5px solid #350072;
            opacity: 1;
        }

        .gm-main-table-sort.is-desc::after {
            border-top: 5px solid #350072;
            border-bottom: 0;
            opacity: 1;
        }

        .gm-main-table tbody tr:last-child td {
            border-bottom: 0;
        }

        .gm-main-table tbody tr:nth-child(even) td {
            background: #fbfcff;
        }

        .gm-main-summary-table {
            min-width: 760px;
        }

        .gm-main-summary-table th,
        .gm-main-summary-table td {
            white-space: normal;
            word-break: break-word;
        }

        .gm-main-graph-panel {
            min-height: 0;
            display: flex;
            flex-direction: column;
            gap: 8px;
            overflow: hidden;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            background: #ffffff;
            padding: 8px;
        }

        .gm-main-graph-panel > canvas {
            flex: 1 1 auto;
            min-height: 260px;
            width: 100% !important;
            height: 100% !important;
        }

        .gm-main-graph-placeholder {
            flex: 0 0 auto;
            border: 1px dashed var(--gm-soft-stroke);
            border-radius: 8px;
            background: #f8fafc;
            color: #6b7280;
            font-size: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            text-align: center;
            padding: 10px;
        }

        .gm-main-graph-placeholder.is-hidden {
            display: none;
        }

        .gm-main-graph-placeholder.is-error {
            border-color: #fecaca;
            background: #fff1f2;
            color: #9f1239;
        }

        .gm-key {
            color: var(--gm-muted);
            margin-right: 6px;
        }

        
        .disclaimer {
            margin: 0;
            margin-top: calc(var(--gm-air-025) - var(--gm-air));
            font-size: 12px;
            color: #6b7280;
            line-height: 1.2;
            text-align: left;
        }

        .disclaimer > * {
            margin: 0;
        }

        .disclaimer a,
        .disclaimer a:visited {
            color: #2b2b2b;
            text-decoration: none;
            border-bottom: 1px solid rgba(43, 43, 43, 0.25);
        }

        .gm-meta-popwrap {
            position: relative;
            display: inline-block;
            max-width: 100%;
        }

        .gm-meta-link,
        .gm-meta-link:visited {
            color: #2b2b2b;
            text-decoration: none;
            border-bottom: 1px solid rgba(43, 43, 43, 0.25);
        }

        .gm-meta-link:focus-visible {
            outline: 2px solid rgba(53, 0, 114, 0.35);
            outline-offset: 2px;
        }

        .gm-meta-tooltip {
            position: absolute;
            top: calc(100% + 6px);
            bottom: auto;
            right: 0;
            left: auto;
            width: min(340px, calc(100vw - 24px));
            max-height: none;
            overflow: visible;
            border: 1px solid var(--gm-soft-stroke);
            border-radius: 8px;
            padding: 8px 10px;
            background: #ffffff;
            text-align: left;
            box-shadow: 0 8px 24px rgba(17, 24, 39, 0.14);
            z-index: 160;
            opacity: 0;
            visibility: hidden;
            transform: translateY(4px);
            transition: opacity 0.14s ease, transform 0.14s ease, visibility 0.14s ease;
            pointer-events: none;
        }

        .gm-meta-popwrap:hover .gm-meta-tooltip,
        .gm-meta-popwrap:focus-within .gm-meta-tooltip {
            opacity: 1;
            visibility: visible;
            transform: translateY(0);
            pointer-events: auto;
        }

        .gm-meta-tooltip-title {
            font-size: 12px;
            font-weight: 700;
            color: #111827;
            margin-bottom: 6px;
            text-align: center;
        }

        .gm-meta-tooltip-section {
            margin-bottom: 7px;
        }

        .gm-meta-tooltip-section:last-child {
            margin-bottom: 0;
        }

        .gm-meta-tooltip-label {
            font-size: 12px;
            font-weight: 700;
            color: #374151;
            margin-bottom: 3px;
        }

        .gm-meta-tooltip-line {
            font-size: 12px;
            color: #4b5563;
            line-height: 1.25;
            margin-bottom: 2px;
            word-break: break-word;
        }

        .gm-meta-tooltip-line.gm-meta-tooltip-line-payload {
            white-space: pre-wrap;
            overflow-wrap: anywhere;
            word-break: break-all;
        }

        .gm-meta-tooltip-line.gm-meta-tooltip-line-contract {
            white-space: pre-wrap;
            overflow-wrap: anywhere;
            word-break: break-word;
        }

        .gm-meta-tooltip-line:last-child {
            margin-bottom: 0;
        }

        .gm-results {
            margin-bottom: var(--gm-air-0125);
        }

        .gm-flowww-note {
            margin-bottom: var(--gm-air-0125);
        }

        .gm-aclaraciones {
            margin-bottom: var(--gm-air-075);
        }

        .gm-designed {
            margin: 0;
        }

        
        .kpis-row {
            display: grid;
            grid-template-columns: calc(35% - (var(--gm-air) * 0.5)) calc(65% - (var(--gm-air) * 0.5));
            gap: 16px;
            align-items: stretch;
            min-width: 0;
            width: 100%;
            padding: 0 16px;
            margin-top: var(--gm-air);
        }

        .kpis-col {
            display: flex;
            align-items: flex-start;
            min-width: 0;
        }

        .kpis-strip {
            width: 100%;
            min-width: 0;
            display: flex;
            align-items: center;
            gap: 0;
        }

        .kpis-boxes {
            flex: 1 1 auto;
            min-width: 0;
            display: flex;
            align-items: stretch;
            gap: var(--gm-air);
        }

        .kpi {
            position: relative;
            flex: 1 1 0;
            min-width: 0;
            height: var(--gm-kpi-height);
            --gm-kpi-bg: #ffffff;
            --gm-kpi-border: var(--gm-soft-stroke);
            --gm-kpi-value-color: var(--gm-text);
            --gm-kpi-title-color: var(--gm-primary);
            border: 1px solid var(--gm-kpi-border);
            border-radius: 9px;
            background: var(--gm-kpi-bg);
            overflow: hidden;
            --gm-kpi-gap: 4px;
            --gm-kpi-shift-y: 1px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 0 8px;
        }

        
        .KPIs_light {
            --gm-kpi-bg: #ffffff;
            --gm-kpi-border: #350072;
            --gm-kpi-value-color: #350072;
            --gm-kpi-title-color: #8072e6;
        }

        .KPIs_dark {
            --gm-kpi-bg: #350072;
            --gm-kpi-border: #350072;
            --gm-kpi-value-color: #ffffff;
            --gm-kpi-title-color: #ffffff;
        }

        .kpi-value {
            position: static;
            display: block;
            text-align: center;
            color: var(--gm-kpi-value-color);
            font-size: 18px;
            font-weight: 700;
            padding: 0;
            line-height: 1;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            width: 100%;
            transform: translateY(var(--gm-kpi-shift-y));
        }

        .kpi-title {
            position: static;
            display: block;
            font-size: 12px;
            color: var(--gm-kpi-title-color);
            line-height: 1;
            text-align: center;
            pointer-events: none;
            background: transparent;
            padding: 0;
            margin-top: var(--gm-kpi-gap);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            width: 100%;
            transform: translateY(var(--gm-kpi-shift-y));
        }

        .kpis-export {
            flex: 0 0 calc(var(--gm-action-btn-width) - var(--gm-action-offset-x));
            width: calc(var(--gm-action-btn-width) - var(--gm-action-offset-x));
            min-width: calc(var(--gm-action-btn-width) - var(--gm-action-offset-x));
            margin-left: var(--gm-action-offset-x);
            margin-right: 0;
        }

        .kpis-export .gm-btn {
            width: 100%;
            height: var(--gm-action-btn-height);
            -webkit-appearance: none;
            appearance: none;
            line-height: 1;
        }

        @media (max-width: 1100px) {
            .main-content {
                grid-template-columns: 1fr;
            }

            .gm-main-layout.has-graph {
                grid-template-columns: 1fr;
            }

            .kpis-row {
                grid-template-columns: 1fr;
                gap: 12px;
            }

            .kpis-strip {
                overflow: hidden;
            }

            .kpis-boxes {
                display: flex;
            }

            .kpi {
                min-width: 0;
            }

        }

        @media (max-width: 900px) {
            .menu .menu-grid {
                grid-template-columns: 1fr 1fr;
                column-gap: 12px;
                row-gap: 12px;
                padding: 0;
            }

            .menu .gm-btn {
                width: 100%;
                height: 46px;
            }

            .menu .gm-field-action {
                margin-left: 0;
            }
        }

        @media (max-width: 760px) {
            .gm-report {
                padding: 8px;
                gap: 0;
            }


            .topbar {
                grid-template-columns: 1fr;
                align-items: start;
                gap: 6px;
            }

            .gm-meta {
                white-space: normal;
            }

            .gm-topbar-center {
                justify-self: stretch;
                width: 100%;
            }

            .gm-topbar-userform {
                width: 100%;
            }

            .gm-topbar-user-inline {
                display: block;
            }

            .gm-topbar-user-label {
                display: block;
                margin-bottom: 2px;
            }

            .gm-topbar-user-wrapper {
                display: block;
                max-width: none;
                width: 100%;
            }

            .gm-topbar-user-trigger {
                width: 100%;
            }

            .gm-topbar-user-menu {
                min-width: 0;
                width: 100%;
                max-width: none;
            }

            .topbar-right {
                justify-self: start;
            }

            .menu .menu-grid {
                grid-template-columns: 1fr;
                row-gap: 12px;
                padding: 0;
            }

            .menu .gm-field-date:first-of-type {
                margin-left: 0;
            }

            .menu .gm-field-action {
                margin-left: 0;
            }

            .gm-main-report-head {
                flex-direction: column;
                align-items: flex-start;
            }

            .gm-main-report-filters {
                text-align: left;
                white-space: normal;
                width: 100%;
                margin-right: 0;
            }

            .gm-main-report-filter-form {
                justify-content: flex-start;
                display: flex;
                width: 100%;
            }

            .gm-main-report-filter-group {
                width: 100%;
                min-width: 0;
                max-width: none;
            }

            .gm-main-report-filter-selectbox,
            .gm-main-report-filter-selectbox .gm-select-trigger {
                max-width: none;
                min-width: 0;
                width: 100%;
            }

            .kpis-row {
                grid-template-columns: 1fr;
            }

            .topbar-right {
                justify-content: flex-start;
            }

            .kpis-strip {
                flex-direction: column;
                align-items: stretch;
                gap: 12px;
            }

            .kpis-export {
                flex: 0 0 auto;
                width: 100%;
                min-width: 0;
                margin-left: 0;
            }

        }

    </style>
</head>
<body<% If bSqlPopupAutoOpen Or bConfigPopupAutoOpen Then Response.Write(" class=""gm-sql-modal-open""") %>>
    <div id="co-loader-overlay">
        <div id="co-loader-card">
            <div id="co-loader-box">
                <div id="co-loader-ring"></div>
                <img id="co-loader-logo" src="<%=Server.HTMLEncode(PathLoaderIconValue)%>" alt="Loader">
            </div>
            <div id="co-loader-title">Growmetrica BI</div>
            <div id="co-loader-msg">Inicializando...</div>
            <div id="co-loader-progress"><div id="co-loader-bar"></div></div>
        </div>
    </div>
    <!-- ------------------------------------------------------------------------------
JAVASCRIPT
------------------------------------------------------------------------------ -->
    <script>
        (function () {
            var overlay, msg, progress;
            try {
                if (!window.sessionStorage) return;
                if (sessionStorage.getItem("gm_loader_pending") !== "1") return;
                overlay = document.getElementById("co-loader-overlay");
                if (!overlay) return;
                msg = document.getElementById("co-loader-msg");
                progress = document.getElementById("co-loader-progress");
                overlay.style.opacity = "1";
                overlay.style.display = "flex";
                if (msg) msg.textContent = "Finalizando...";
                if (progress) progress.style.display = "none";
                if (document.body) document.body.classList.add("co-lock-scroll");
            } catch (e) {}
        })();
    </script>
    <div class="gm-report">
        
        <div class="topbar">
            <div class="gm-meta topbar-left"><%=Server.HTMLEncode(EngineTitle)%></div>
            <div class="gm-topbar-center">
                <% If AuthIsSuperAdmin Then %>
                <form class="gm-topbar-userform" method="post" action="">
                    <input type="hidden" name="action" value="">
                    <input type="hidden" name="report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
                    <input type="hidden" name="clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
                    <input type="hidden" name="start_date" value="<%=Server.HTMLEncode(sStartDate)%>">
                    <input type="hidden" name="end_date" value="<%=Server.HTMLEncode(sEndDate)%>">
                    <input type="hidden" name="filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
                    <input type="hidden" name="filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
                    <input type="hidden" name="filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
                    <input type="hidden" name="ctx_css_px" id="gm-topbar-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
                    <input type="hidden" name="ctx_client_now" id="gm-topbar-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
                    <input type="hidden" name="ctx_client_tz" id="gm-topbar-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
                    <input type="hidden" name="ctx_client_tz_offset_min" id="gm-topbar-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
                    <div class="gm-topbar-user-inline">
                        <span class="gm-topbar-user-label">Selector Super Admin:</span>
                        <div class="gm-topbar-user-wrapper" id="gm-user-dropdown">
                            <button type="button" class="gm-topbar-user-trigger" id="gm-user-trigger" aria-haspopup="listbox" aria-expanded="false">
                                <span class="gm-topbar-user-text" id="gm-user-display"></span>
                            </button>
                            <div class="gm-topbar-user-menu" id="gm-user-menu" role="listbox" aria-label="Seleccion de usuario"></div>
                            <select id="topbar_user_filter" name="user_filter" onchange="if(window.gmSubmitFormWithLoader){window.gmSubmitFormWithLoader(this.form,'TECH');}else{if(window.gmUpdateClientContext){window.gmUpdateClientContext();}this.form.submit();}">
                                <%=sUserFilterHTML%>
                            </select>
                        </div>
                    </div>
                </form>
                <% End If %>
            </div>
            <div class="topbar-right">
                <% If bShowSqlCodeLink Then %>
                <a href="#" id="gm-master-popup-link">Master</a>
                <a href="#" id="gm-modules-popup-link">Modules</a>
                <a href="#" id="gm-home-popup-link">HConfig</a>
                <a href="#" id="gm-config-popup-link">RConfig</a>
                <a href="#" id="gm-filters-sql-popup-link">Filters</a>
                <a href="#" id="gm-sql-popup-link">Data</a>
                <a href="#" id="gm-kpis-sql-popup-link">KPIs</a>
                <a href="#" id="gm-graph-sql-popup-link">Graph</a>
                <a href="#" id="gm-gconfig-popup-link">GConfig</a>
                <% End If %>
                <% If AuthIsSuperAdmin Then %>
                <span class="gm-meta-popwrap gm-topbar-meta-popwrap">
                    <a href="#" class="gm-meta-link" aria-controls="gm-webhook-tooltip">Log</a>
                    <div class="gm-meta-tooltip" id="gm-webhook-tooltip">
                        <div class="gm-meta-tooltip-title"><%=Server.HTMLEncode(MsgAdmLogLastActionTitle)%> <%=Server.HTMLEncode(TxtLogExecutionTimestamp)%></div>
                        <div class="gm-meta-tooltip-line gm-meta-tooltip-line-payload"><%=Server.HTMLEncode(TxtWebhookPayload)%></div>
                    </div>
                </span>
                <% End If %>
                <span class="gm-meta-popwrap gm-topbar-meta-popwrap">
                    <a href="#" id="gm-meta-toggle" class="gm-meta-link" aria-controls="gm-meta-tooltip">Metadatos</a>
                    <div class="gm-meta-tooltip" id="gm-meta-tooltip">
                        <div class="gm-meta-tooltip-title">Detalle de metadatos de sesion</div>
                        <div class="gm-meta-tooltip-line gm-meta-tooltip-line-contract" id="gm-meta-contract-text"><%=Server.HTMLEncode(TxtMetadatos)%></div>
                    </div>
                </span>
                <a href="#" id="gm-fullscreen-link">FullScreen</a>
            </div>
        </div>

        
        <form class="gm-modal menu" method="post" action="" onsubmit="if(window.gmSubmitFormWithLoader){return window.gmSubmitFormWithLoader(this,'TECH');}if(window.gmUpdateClientContext){window.gmUpdateClientContext();}return true;">
            <input type="hidden" name="action" value="">
            <input type="hidden" name="user_filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
            <input type="hidden" name="filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
            <input type="hidden" name="filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
            <input type="hidden" name="filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
            <input type="hidden" name="ctx_css_px" id="gm-menu-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
            <input type="hidden" name="ctx_client_now" id="gm-menu-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
            <input type="hidden" name="ctx_client_tz" id="gm-menu-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
            <input type="hidden" name="ctx_client_tz_offset_min" id="gm-menu-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
            <div class="menu-grid">
                <div class="gm-field">
                    <label class="gm-label" for="report">Reporte</label>
                    <div class="gm-selectbox" id="gm-report-dropdown">
                        <button type="button" class="gm-select-trigger" id="gm-report-trigger" aria-haspopup="listbox" aria-expanded="false">
                            <span class="gm-select-text" id="gm-report-display"></span>
                        </button>
                        <div class="gm-select-menu" id="gm-report-menu" role="listbox" aria-label="Seleccion de reporte"></div>
                        <select class="gm-control" id="report" name="report">
                            <%=sReportOptionsHTML%>
                        </select>
                    </div>
                </div>

                <div class="gm-field">
                    <label class="gm-label" for="clinic">Sucursal</label>
                    <div class="gm-selectbox" id="gm-clinic-dropdown">
                        <button type="button" class="gm-select-trigger" id="gm-clinic-trigger" aria-haspopup="listbox" aria-expanded="false">
                            <span class="gm-select-text" id="gm-clinic-display"></span>
                        </button>
                        <div class="gm-select-menu" id="gm-clinic-menu" role="listbox" aria-label="Seleccion de sucursal"></div>
                        <select class="gm-control" id="clinic" name="clinic">
                            <%=sClinicHTML%>
                        </select>
                    </div>
                </div>

                <div class="gm-field gm-field-date">
                    <label class="gm-label" for="start_date">Fecha Inicial</label>
                    <input class="gm-control" id="start_date" name="start_date" type="date" min="<%=Server.HTMLEncode(EffectiveAllowedMinDateISO)%>" max="<%=Server.HTMLEncode(EffectiveAllowedMaxDateISO)%>" value="<%=Server.HTMLEncode(sStartDate)%>">
                </div>

                <div class="gm-field gm-field-date">
                    <label class="gm-label" for="end_date">Fecha Final</label>
                    <input class="gm-control" id="end_date" name="end_date" type="date" min="<%=Server.HTMLEncode(EffectiveAllowedMinDateISO)%>" max="<%=Server.HTMLEncode(EffectiveAllowedMaxDateISO)%>" value="<%=Server.HTMLEncode(sEndDate)%>">
                </div>

                <div class="gm-field gm-field-action">
                    <button class="gm-btn" type="submit">Mostrar</button>
                </div>
            </div>
        </form>

        
        <section class="gm-modal main">
            <div class="main-content is-report1">
                <div class="gm-main-report-head">
                    <div class="gm-main-report-title"><%=Server.HTMLEncode(ReportTitle)%></div>
                    <% If ReportHasFilters Then %>
                    <div class="gm-main-report-filters">
                        <form class="gm-main-report-filter-form" method="post" action="">
                            <input type="hidden" name="action" value="">
                            <input type="hidden" name="user_filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
                            <input type="hidden" name="report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
                            <input type="hidden" name="clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
                            <input type="hidden" name="start_date" value="<%=Server.HTMLEncode(sStartDate)%>">
                            <input type="hidden" name="end_date" value="<%=Server.HTMLEncode(sEndDate)%>">
                            <input type="hidden" name="ctx_css_px" id="gm-filter-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
                            <input type="hidden" name="ctx_client_now" id="gm-filter-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
                            <input type="hidden" name="ctx_client_tz" id="gm-filter-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
                            <input type="hidden" name="ctx_client_tz_offset_min" id="gm-filter-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">

                            <% If Len(sFilter1OptionsHTML) > 0 Then %>
                            <span class="gm-main-report-filter-group">
                                <label class="gm-main-report-filter-label" for="gm-filter1-select"><%=Server.HTMLEncode(sFilter1Title)%></label>
                                <div class="gm-main-report-filter-selectbox gm-selectbox" id="gm-filter1-dropdown">
                                    <button type="button" class="gm-select-trigger" id="gm-filter1-trigger" aria-haspopup="listbox" aria-expanded="false">
                                        <span class="gm-select-text" id="gm-filter1-display"></span>
                                    </button>
                                    <div class="gm-select-menu" id="gm-filter1-menu" role="listbox" aria-label="Seleccion de filtro 1"></div>
                                    <select class="gm-main-report-filter-control" id="gm-filter1-select" name="filter1"><%=sFilter1OptionsHTML%></select>
                                </div>
                            </span>
                            <% Else %>
                            <input type="hidden" name="filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
                            <% End If %>

                            <% If Len(sFilter2OptionsHTML) > 0 Then %>
                            <span class="gm-main-report-filter-group">
                                <label class="gm-main-report-filter-label" for="gm-filter2-select"><%=Server.HTMLEncode(sFilter2Title)%></label>
                                <div class="gm-main-report-filter-selectbox gm-selectbox" id="gm-filter2-dropdown">
                                    <button type="button" class="gm-select-trigger" id="gm-filter2-trigger" aria-haspopup="listbox" aria-expanded="false">
                                        <span class="gm-select-text" id="gm-filter2-display"></span>
                                    </button>
                                    <div class="gm-select-menu" id="gm-filter2-menu" role="listbox" aria-label="Seleccion de filtro 2"></div>
                                    <select class="gm-main-report-filter-control" id="gm-filter2-select" name="filter2"><%=sFilter2OptionsHTML%></select>
                                </div>
                            </span>
                            <% Else %>
                            <input type="hidden" name="filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
                            <% End If %>

                            <% If Len(sFilter3OptionsHTML) > 0 Then %>
                            <span class="gm-main-report-filter-group">
                                <label class="gm-main-report-filter-label" for="gm-filter3-select"><%=Server.HTMLEncode(sFilter3Title)%></label>
                                <div class="gm-main-report-filter-selectbox gm-selectbox" id="gm-filter3-dropdown">
                                    <button type="button" class="gm-select-trigger" id="gm-filter3-trigger" aria-haspopup="listbox" aria-expanded="false">
                                        <span class="gm-select-text" id="gm-filter3-display"></span>
                                    </button>
                                    <div class="gm-select-menu" id="gm-filter3-menu" role="listbox" aria-label="Seleccion de filtro 3"></div>
                                    <select class="gm-main-report-filter-control" id="gm-filter3-select" name="filter3"><%=sFilter3OptionsHTML%></select>
                                </div>
                            </span>
                            <% Else %>
                            <input type="hidden" name="filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
                            <% End If %>
                        </form>
                    </div>
                    <% End If %>
                </div>

                <div id="LayoutMain-Container" class="gm-main-layout<% If ReportHasGraph Then Response.Write(" has-graph") %>">
                    <% If ReportHasGraph Then %>
                    <aside id="CmpGraph" class="gm-main-graph-panel">
                        <canvas id="gm-main-graph-canvas" aria-label="Grafica del reporte"></canvas>
                        <div class="gm-main-graph-placeholder is-hidden" id="gm-main-graph-placeholder"></div>
                        <script type="application/json" id="gm-graph-data-json"><%=Replace(CStr(GraphDataJson), "</", "<\/")%></script>
                        <script type="application/json" id="gm-graph-config-json"><%=Replace(CStr(GraphConfigJson), "</", "<\/")%></script>
                    </aside>
                    <% End If %>

                    <div id="CmpMainTable" class="gm-main-table-shell">
                        <div class="gm-main-table-wrap<% If ReportDataHorizontalScroll Then Response.Write(" gm-hscroll") Else Response.Write(" gm-no-hscroll") End If %>">
                            <div class="gm-main-table-vscroll">
                                <table class="gm-main-table" id="gm-report1-table">
                                    <thead>
                                        <tr>
                                            <%
                                            Dim iField
                                            If Not rsData Is Nothing Then
                                                If rsData.State = 1 Then
                                                    For iField = 0 To rsData.Fields.Count - 1
                                            %>
                                            <th aria-sort="none"><button class="gm-main-table-sort" type="button" data-type="text"><%=Server.HTMLEncode(CStr(rsData.Fields(iField).Name))%></button></th>
                                            <%
                                                    Next
                                                End If
                                            End If
                                            %>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <%
                                        Dim bMainHasRows, jField, vCell
                                        bMainHasRows = False
                                        If Not rsData Is Nothing Then
                                            If rsData.State = 1 And Not rsData.EOF Then
                                                Do While Not rsData.EOF
                                                    bMainHasRows = True
                                        %>
                                        <tr>
                                            <%
                                            For jField = 0 To rsData.Fields.Count - 1
                                                vCell = "-"
                                                On Error Resume Next
                                                vCell = CStr(rsData.Fields(jField).Value)
                                                If Err.Number <> 0 Then
                                                    vCell = "-"
                                                    Err.Clear
                                                End If
                                                On Error GoTo 0
                                            %>
                                            <td><%=Server.HTMLEncode(vCell)%></td>
                                            <% Next %>
                                        </tr>
                                        <%
                                                    rsData.MoveNext
                                                Loop
                                            End If
                                        End If
                                        If Not bMainHasRows Then
                                        %>
                                        <tr>
                                            <td colspan="16"><%=Server.HTMLEncode(MsgStdTableNoData)%></td>
                                        </tr>
                                        <%
                                        End If
                                        %>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </section>

        
        <div class="kpis-row">
            <div class="disclaimer">
                <div class="gm-results"><%=Server.HTMLEncode(DisclaimerText1)%></div>
                <div class="gm-flowww-note"><%=Server.HTMLEncode(DisclaimerText2)%></div>
                <div class="gm-aclaraciones">
                    <%=Server.HTMLEncode(UIFooterDisclaimerText3Value)%>&nbsp;&nbsp;
                    <a href="<%=Server.HTMLEncode(PolicyLink_SKG)%>" target="_blank">Politica vigente</a>
                    &nbsp;&nbsp;
                    <a href="<%=sWaOperacionesUrl%>" target="_blank">Operaciones</a>
                    &nbsp;&nbsp;
                    <a href="<%=sWaSoporteUrl%>" target="_blank" id="gm-wa-soporte">Soporte</a>
                </div>
                <div class="gm-designed"><%=Server.HTMLEncode(AuthorText)%></div>
            </div>
            <div class="kpis-col">
                <div class="kpis-strip">
                    <% If ReportHasKPIs Then %>
                    <div class="kpis-boxes">
                        <%
                        Dim bKPIHasRows, kpiClass
                        bKPIHasRows = False
                        If ReportHasKPIs And Not rsKPIs Is Nothing Then
                            If rsKPIs.State = 1 And Not rsKPIs.EOF Then
                                Do While Not rsKPIs.EOF
                                    bKPIHasRows = True
                                    kpiClass = "KPIs_light"
                                    If UCase(CStr(rsKPIs("KPIScheme"))) = "TRUE" Or CStr(rsKPIs("KPIScheme")) = "-1" Or CStr(rsKPIs("KPIScheme")) = "1" Then
                                        kpiClass = "KPIs_dark"
                                    End If
                        %>
                        <div class="kpi <%=kpiClass%>">
                            <div class="kpi-value"><%=Server.HTMLEncode(CStr(rsKPIs("KPIValue")))%></div>
                            <div class="kpi-title"><%=Server.HTMLEncode(CStr(rsKPIs("KPIName")))%></div>
                        </div>
                        <%
                                    rsKPIs.MoveNext
                                Loop
                            End If
                        End If
                        If ReportHasKPIs And Not bKPIHasRows Then
                        %>
                        <div class="kpi KPIs_light">
                            <div class="kpi-value">-</div>
                            <div class="kpi-title"><%=Server.HTMLEncode(MsgStdKpiNoData)%></div>
                        </div>
                        <%
                        End If
                        %>
                    </div>
                    <% End If %>
                    <div class="kpis-export">
                        <button class="gm-btn" type="submit" form="gm-export-form" id="gm-export-btn">Exportar</button>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <form id="gm-export-form" method="post" action="" style="display:none;">
        <input type="hidden" name="action" value="export">
        <input type="hidden" name="user_filter" id="gm-export-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-export-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-export-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-export-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-export-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-export-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-export-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-export-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-export-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-export-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-export-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-export-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>

    <% If bShowSqlCodeLink Then %>
    <form id="gm-config-popup-form" method="post" action="" style="display:none;">
        <input type="hidden" name="open_config_popup" value="1">
        <input type="hidden" name="config_popup_kind" id="gm-config-popup-kind" value="reports">
        <input type="hidden" name="action" value="">
        <input type="hidden" name="user_filter" id="gm-config-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-config-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-config-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-config-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-config-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-config-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-config-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-config-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-config-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-config-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-config-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-config-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>

    <form id="gm-sql-popup-form" method="post" action="" style="display:none;">
        <input type="hidden" name="open_sql_popup" value="1">
        <input type="hidden" name="sql_popup_kind" value="data">
        <input type="hidden" name="action" value="">
        <input type="hidden" name="user_filter" id="gm-sql-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-sql-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-sql-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-sql-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-sql-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-sql-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-sql-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-sql-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-sql-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-sql-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-sql-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-sql-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>

    <form id="gm-filters-sql-popup-form" method="post" action="" style="display:none;">
        <input type="hidden" name="open_sql_popup" value="1">
        <input type="hidden" name="sql_popup_kind" value="filters">
        <input type="hidden" name="action" value="">
        <input type="hidden" name="user_filter" id="gm-filters-sql-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-filters-sql-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-filters-sql-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-filters-sql-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-filters-sql-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-filters-sql-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-filters-sql-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-filters-sql-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-filters-sql-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-filters-sql-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-filters-sql-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-filters-sql-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>

    <form id="gm-kpis-sql-popup-form" method="post" action="" style="display:none;">
        <input type="hidden" name="open_sql_popup" value="1">
        <input type="hidden" name="sql_popup_kind" value="kpis">
        <input type="hidden" name="action" value="">
        <input type="hidden" name="user_filter" id="gm-kpis-sql-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-kpis-sql-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-kpis-sql-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-kpis-sql-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-kpis-sql-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-kpis-sql-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-kpis-sql-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-kpis-sql-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-kpis-sql-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-kpis-sql-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-kpis-sql-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-kpis-sql-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>

    <form id="gm-graph-sql-popup-form" method="post" action="" style="display:none;">
        <input type="hidden" name="open_sql_popup" value="1">
        <input type="hidden" name="sql_popup_kind" value="graph">
        <input type="hidden" name="action" value="">
        <input type="hidden" name="user_filter" id="gm-graph-sql-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-graph-sql-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-graph-sql-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-graph-sql-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-graph-sql-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-graph-sql-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-graph-sql-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-graph-sql-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-graph-sql-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-graph-sql-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-graph-sql-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-graph-sql-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>

    <form id="gm-gconfig-popup-form" method="post" action="" style="display:none;">
        <input type="hidden" name="open_config_popup" value="1">
        <input type="hidden" name="config_popup_kind" value="graph">
        <input type="hidden" name="action" value="">
        <input type="hidden" name="user_filter" id="gm-gconfig-user-filter" value="<%=Server.HTMLEncode(sSelectedUserFilter)%>">
        <input type="hidden" name="report" id="gm-gconfig-report" value="<%=Server.HTMLEncode(sSelectedReportID)%>">
        <input type="hidden" name="clinic" id="gm-gconfig-clinic" value="<%=Server.HTMLEncode(sSelectedClinic)%>">
        <input type="hidden" name="start_date" id="gm-gconfig-start-date" value="<%=Server.HTMLEncode(sStartDate)%>">
        <input type="hidden" name="end_date" id="gm-gconfig-end-date" value="<%=Server.HTMLEncode(sEndDate)%>">
        <input type="hidden" name="filter1" id="gm-gconfig-filter1" value="<%=Server.HTMLEncode(sFilter1IDs)%>">
        <input type="hidden" name="filter2" id="gm-gconfig-filter2" value="<%=Server.HTMLEncode(sFilter2IDs)%>">
        <input type="hidden" name="filter3" id="gm-gconfig-filter3" value="<%=Server.HTMLEncode(sFilter3IDs)%>">
        <input type="hidden" name="ctx_css_px" id="gm-gconfig-ctx-css-px" value="<%=Server.HTMLEncode(CtxCSSpx)%>">
        <input type="hidden" name="ctx_client_now" id="gm-gconfig-ctx-client-now" value="<%=Server.HTMLEncode(sCtxClientNow)%>">
        <input type="hidden" name="ctx_client_tz" id="gm-gconfig-ctx-client-tz" value="<%=Server.HTMLEncode(sCtxClientTZ)%>">
        <input type="hidden" name="ctx_client_tz_offset_min" id="gm-gconfig-ctx-client-offset" value="<%=Server.HTMLEncode(Trim(CStr(Request("ctx_client_tz_offset_min"))))%>">
    </form>
    <% End If %>

    <div class="gm-sql-modal<% If bConfigPopupAutoOpen Then Response.Write(" is-open") %>" id="gm-config-modal" role="dialog" aria-modal="true" aria-labelledby="gm-config-modal-title">
        <div class="gm-sql-modal-dialog" role="document">
            <div class="gm-sql-modal-head">
                <div class="gm-sql-modal-title" id="gm-config-modal-title"><% If Len(Trim(ConfigPopupStatusIcon)) > 0 Then Response.Write(ConfigPopupStatusIcon & " ") %><%=Server.HTMLEncode(ConfigPopupUrl)%></div>
                <div class="gm-sql-modal-actions">
                    <button type="button" class="gm-sql-modal-copy" id="gm-config-modal-copy">Copy</button>
                    <button type="button" class="gm-sql-modal-close" id="gm-config-modal-close">Close</button>
                </div>
            </div>
            <div class="gm-sql-modal-body gm-config-modal-body">
                <div class="gm-config-table-wrap" id="gm-config-table-wrap">
                    <div class="gm-config-table-host" id="gm-config-modal-content"><%=Server.HTMLEncode(ConfigPopupContent)%></div>
                </div>
            </div>
        </div>
    </div>

    <div class="gm-sql-modal<% If bSqlPopupAutoOpen Then Response.Write(" is-open") %>" id="gm-sql-modal" role="dialog" aria-modal="true" aria-labelledby="gm-sql-modal-title">
        <div class="gm-sql-modal-dialog" role="document">
            <div class="gm-sql-modal-head">
                <div class="gm-sql-modal-title" id="gm-sql-modal-title"><% If Len(Trim(SqlPopupStatusIcon)) > 0 Then Response.Write(SqlPopupStatusIcon & " ") %><%=Server.HTMLEncode(SqlPopupUrl)%></div>
                <div class="gm-sql-modal-actions">
                    <button type="button" class="gm-sql-modal-copy" id="gm-sql-modal-copy">Copy</button>
                    <button type="button" class="gm-sql-modal-close" id="gm-sql-modal-close">Close</button>
                </div>
            </div>
            <div class="gm-sql-modal-body">
                <pre class="gm-sql-modal-code" id="gm-sql-modal-content"><%=Server.HTMLEncode(SqlPopupContent)%></pre>
            </div>
        </div>
    </div>

    <!-- ------------------------------------------------------------------------------
JAVASCRIPT
------------------------------------------------------------------------------ -->
    <script>
        (function () {
            var waSoporte = document.getElementById("gm-wa-soporte");
            var metaToggle = document.getElementById("gm-meta-toggle");
            var fullScreenLink = document.getElementById("gm-fullscreen-link");
            var metaContract = document.getElementById("gm-meta-contract-text");
            var masterPopupLink = document.getElementById("gm-master-popup-link");
            var modulesPopupLink = document.getElementById("gm-modules-popup-link");
            var homePopupLink = document.getElementById("gm-home-popup-link");
            var configPopupLink = document.getElementById("gm-config-popup-link");
            var configPopupForm = document.getElementById("gm-config-popup-form");
            var configModal = document.getElementById("gm-config-modal");
            var configModalTitle = document.getElementById("gm-config-modal-title");
            var configModalContent = document.getElementById("gm-config-modal-content");
            var configModalCopy = document.getElementById("gm-config-modal-copy");
            var configModalClose = document.getElementById("gm-config-modal-close");
            var gConfigPopupLink = document.getElementById("gm-gconfig-popup-link");
            var gConfigPopupForm = document.getElementById("gm-gconfig-popup-form");
            var filtersSqlPopupLink = document.getElementById("gm-filters-sql-popup-link");
            var filtersSqlPopupForm = document.getElementById("gm-filters-sql-popup-form");
            var sqlPopupLink = document.getElementById("gm-sql-popup-link");
            var sqlPopupForm = document.getElementById("gm-sql-popup-form");
            var kpisSqlPopupLink = document.getElementById("gm-kpis-sql-popup-link");
            var kpisSqlPopupForm = document.getElementById("gm-kpis-sql-popup-form");
            var graphSqlPopupLink = document.getElementById("gm-graph-sql-popup-link");
            var graphSqlPopupForm = document.getElementById("gm-graph-sql-popup-form");
            var sqlModal = document.getElementById("gm-sql-modal");
            var sqlModalTitle = document.getElementById("gm-sql-modal-title");
            var sqlModalContent = document.getElementById("gm-sql-modal-content");
            var sqlModalCopy = document.getElementById("gm-sql-modal-copy");
            var sqlModalClose = document.getElementById("gm-sql-modal-close");
            var configRawContent = "";
            var configStickyRaf = null;
            var graphCanvas = document.getElementById("gm-main-graph-canvas");
            var graphDataNode = document.getElementById("gm-graph-data-json");
            var graphConfigNode = document.getElementById("gm-graph-config-json");
            var graphPlaceholder = document.getElementById("gm-main-graph-placeholder");
            var graphChartInstance = null;
            var chartJsIsLoading = false;
            var chartJsWaitQueue = [];
            var gmMsgStdGraphNoData = "<%=GmJsonEscape(CStr(MsgStdGraphNoData))%>";
            var gmMsgStdGraphSqlBuildFailed = "<%=GmJsonEscape(CStr(MsgStdGraphSqlBuildFailed))%>";
            var gmMsgStdGraphLibLoadFailed = "<%=GmJsonEscape(CStr(MsgStdGraphLibLoadFailed))%>";
            var gmMsgStdGraphCanvasInitFailed = "<%=GmJsonEscape(CStr(MsgStdGraphCanvasInitFailed))%>";
            var gmWaSupportTemplate = "<%=GmJsonEscape(CStr(WASupportTemplateValue))%>";
            var gmLoaderLogoUrl = "<%=GmJsonEscape(CStr(PathLoaderIconValue))%>";
            var gmLoaderSafetyTimeoutMs = <%=CStr(ToLongOrZero(nLoaderSafetyTimeoutMs))%>;
            var gmLoaderHideTimer = 0;
            var gmLoaderPhaseTimer = 0;
            var gmLoaderAutoPhaseTimers = [];
            var gmLoaderTrickleTimer = 0;
            var gmLoaderTweenRaf = 0;
            var gmLoaderCurrentPct = 0;

            function gmGetLoaderHostDocument() {
                return document;
            }

            function gmEnsureLoaderStyle(hostDoc) {
                var styleEl;
                if (!hostDoc) return;
                styleEl = hostDoc.getElementById("co-loader-style");
                if (styleEl) return;
                styleEl = hostDoc.createElement("style");
                styleEl.id = "co-loader-style";
                styleEl.textContent =
                    "#co-loader-overlay{position:fixed;inset:0;z-index:2147483647;display:none;align-items:center;justify-content:center;background:transparent;-webkit-backdrop-filter:blur(6px);backdrop-filter:blur(6px);}" +
                    "#co-loader-card{width:320px;padding:22px;background:transparent;border-radius:14px;box-shadow:none;text-align:center;font-family:'DM Sans','Open Sans',Arial,sans-serif;}" +
                    "#co-loader-title{font-size:16px;font-weight:700;color:#111827;margin:0 0 8px 0;line-height:1.2;}" +
                    "#co-loader-box{position:relative;width:56px;height:56px;margin:0 auto 8px auto;}" +
                    "#co-loader-logo{width:36px;height:36px;border-radius:50%;object-fit:contain;background:transparent;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);z-index:2;}" +
                    "#co-loader-ring{position:absolute;inset:0;border-radius:50%;border:3px solid #d1d5db;border-top-color:#111827;border-right-color:#374151;animation:co-spin 1s linear infinite;}" +
                    "#co-loader-msg{font-family:'DM Sans','Open Sans',Arial,sans-serif;font-size:12px;line-height:1.2;color:#6b7280;margin-top:6px;opacity:1;}" +
                    "#co-loader-progress{margin-top:10px;height:8px;background:#eee;border-radius:8px;overflow:hidden;}" +
                    "#co-loader-bar{height:100%;width:0%;background:#111827;transition:width .35s ease;}" +
                    ".co-no-backdrop #co-loader-overlay{background:#f1f1f5;}" +
                    "@keyframes co-spin{to{transform:rotate(360deg);}}" +
                    ".co-lock-scroll{overflow:hidden !important;}";
                hostDoc.head.appendChild(styleEl);

                if (window.CSS && CSS.supports) {
                    if (!(CSS.supports("backdrop-filter", "blur(1px)") || CSS.supports("-webkit-backdrop-filter", "blur(1px)"))) {
                        hostDoc.documentElement.classList.add("co-no-backdrop");
                    }
                }
            }

            function gmSetLoaderPhase(phaseCode) {
                var hostDoc = gmGetLoaderHostDocument();
                var msgEl = hostDoc.getElementById("co-loader-msg");
                var barEl = hostDoc.getElementById("co-loader-bar");
                var phase = String(phaseCode || "").toUpperCase();
                var msgText = "Inicializando...";
                var pct = 6;
                var durMs = 260;

                function stopTween() {
                    if (gmLoaderTweenRaf) {
                        window.cancelAnimationFrame(gmLoaderTweenRaf);
                        gmLoaderTweenRaf = 0;
                    }
                }

                function stopTrickle() {
                    if (gmLoaderTrickleTimer) {
                        window.clearInterval(gmLoaderTrickleTimer);
                        gmLoaderTrickleTimer = 0;
                    }
                }

                function animateTo(targetPct, durationMs) {
                    var startPct = gmLoaderCurrentPct;
                    var startTs = 0;
                    var target = targetPct;

                    if (!barEl) return;
                    if (!isFinite(startPct) || startPct < 0) startPct = 0;
                    if (!isFinite(target) || target < 0) target = 0;
                    if (target > 100) target = 100;
                    if (!durationMs || durationMs <= 0) {
                        gmLoaderCurrentPct = target;
                        barEl.style.width = String(target) + "%";
                        return;
                    }

                    stopTween();
                    function step(ts) {
                        var p, eased, nextPct;
                        if (!startTs) startTs = ts;
                        p = (ts - startTs) / durationMs;
                        if (p > 1) p = 1;
                        eased = 1 - Math.pow(1 - p, 3);
                        nextPct = startPct + ((target - startPct) * eased);
                        gmLoaderCurrentPct = nextPct;
                        barEl.style.width = String(nextPct) + "%";
                        if (p < 1) gmLoaderTweenRaf = window.requestAnimationFrame(step);
                    }
                    gmLoaderTweenRaf = window.requestAnimationFrame(step);
                }

                if (!msgEl || !barEl) return;
                stopTrickle();

                if (phase === "TECH") { msgText = "Preparando entorno..."; pct = 18; durMs = 380; }
                else if (phase === "PREP") { msgText = "Cargando configuracion..."; pct = 38; durMs = 700; }
                else if (phase === "SQL") { msgText = "Consultando datos..."; pct = 72; durMs = 900; }
                else if (phase === "RENDER") { msgText = "Preparando visualizacion..."; pct = 88; durMs = 700; }
                else if (phase === "LOG") { msgText = "Finalizando..."; pct = 100; durMs = 180; }

                msgEl.textContent = msgText;
                animateTo(pct, durMs);

                if (phase === "RENDER") {
                    gmLoaderTrickleTimer = window.setInterval(function () {
                        var nextPct = gmLoaderCurrentPct + 0.9;
                        if (nextPct > 96) nextPct = 96;
                        if (gmLoaderCurrentPct < 96) animateTo(nextPct, 260);
                    }, 220);
                }
            }

            function gmClearLoaderAutoPhaseTimers() {
                var i;
                for (i = 0; i < gmLoaderAutoPhaseTimers.length; i++) {
                    window.clearTimeout(gmLoaderAutoPhaseTimers[i]);
                }
                gmLoaderAutoPhaseTimers = [];
                if (gmLoaderTrickleTimer) {
                    window.clearInterval(gmLoaderTrickleTimer);
                    gmLoaderTrickleTimer = 0;
                }
                if (gmLoaderTweenRaf) {
                    window.cancelAnimationFrame(gmLoaderTweenRaf);
                    gmLoaderTweenRaf = 0;
                }
            }

            function gmStartLoaderAutoPhases() {
                gmClearLoaderAutoPhaseTimers();
                gmLoaderAutoPhaseTimers.push(window.setTimeout(function () {
                    if (window.GM_PHASE) window.GM_PHASE("PREP");
                }, 220));
                gmLoaderAutoPhaseTimers.push(window.setTimeout(function () {
                    if (window.GM_PHASE) window.GM_PHASE("SQL");
                }, 620));
                gmLoaderAutoPhaseTimers.push(window.setTimeout(function () {
                    if (window.GM_PHASE) window.GM_PHASE("RENDER");
                }, 1200));
            }

            function gmShowLoader(phaseCode) {
                var hostDoc = gmGetLoaderHostDocument();
                var overlay = hostDoc.getElementById("co-loader-overlay");
                var card, box, ring, logo, title, msg, progress, bar, existingBar, progressEl;
                if (gmLoaderPhaseTimer) {
                    window.clearTimeout(gmLoaderPhaseTimer);
                    gmLoaderPhaseTimer = 0;
                }
                gmEnsureLoaderStyle(hostDoc);
                if (!overlay) {
                    overlay = hostDoc.createElement("div");
                    overlay.id = "co-loader-overlay";

                    card = hostDoc.createElement("div");
                    card.id = "co-loader-card";
                    box = hostDoc.createElement("div");
                    box.id = "co-loader-box";
                    ring = hostDoc.createElement("div");
                    ring.id = "co-loader-ring";
                    logo = hostDoc.createElement("img");
                    logo.id = "co-loader-logo";
                    logo.alt = "Loader";
                    logo.src = gmLoaderLogoUrl;
                    title = hostDoc.createElement("div");
                    title.id = "co-loader-title";
                    title.textContent = "Growmetrica BI";
                    msg = hostDoc.createElement("div");
                    msg.id = "co-loader-msg";
                    msg.textContent = "Inicializando...";
                    progress = hostDoc.createElement("div");
                    progress.id = "co-loader-progress";
                    bar = hostDoc.createElement("div");
                    bar.id = "co-loader-bar";

                    progress.appendChild(bar);
                    box.appendChild(ring);
                    box.appendChild(logo);
                    card.appendChild(box);
                    card.appendChild(title);
                    card.appendChild(msg);
                    card.appendChild(progress);
                    overlay.appendChild(card);
                    hostDoc.documentElement.appendChild(overlay);
                }
                overlay.style.opacity = "1";
                overlay.style.display = "flex";

                progressEl = hostDoc.getElementById("co-loader-progress");
                if (progressEl) progressEl.style.display = "";
                existingBar = hostDoc.getElementById("co-loader-bar");
                if (existingBar) existingBar.style.width = "0%";
                gmLoaderCurrentPct = 0;

                if (hostDoc.body) hostDoc.body.classList.add("co-lock-scroll");
                gmSetLoaderPhase(phaseCode || "TECH");
                gmStartLoaderAutoPhases();

                if (gmLoaderHideTimer) window.clearTimeout(gmLoaderHideTimer);
                if (!gmLoaderSafetyTimeoutMs || gmLoaderSafetyTimeoutMs < 1) gmLoaderSafetyTimeoutMs = 25000;
                gmLoaderHideTimer = window.setTimeout(function () {
                    if (window.GM_LOADER_DONE) window.GM_LOADER_DONE();
                }, gmLoaderSafetyTimeoutMs);
            }

            function gmHideLoader() {
                var hostDoc = gmGetLoaderHostDocument();
                var overlay = hostDoc.getElementById("co-loader-overlay");
                function removeOverlayNow() {
                    try { overlay.remove(); } catch (e) {}
                    if (hostDoc.body) hostDoc.body.classList.remove("co-lock-scroll");
                    gmLoaderCurrentPct = 0;
                    try { if (window.sessionStorage) sessionStorage.removeItem("gm_loader_pending"); } catch (e3) {}
                }
                if (gmLoaderHideTimer) {
                    window.clearTimeout(gmLoaderHideTimer);
                    gmLoaderHideTimer = 0;
                }
                if (gmLoaderPhaseTimer) {
                    window.clearTimeout(gmLoaderPhaseTimer);
                    gmLoaderPhaseTimer = 0;
                }
                gmClearLoaderAutoPhaseTimers();
                if (!overlay) {
                    if (hostDoc.body) hostDoc.body.classList.remove("co-lock-scroll");
                    try { if (window.sessionStorage) sessionStorage.removeItem("gm_loader_pending"); } catch (e5) {}
                    return;
                }
                if (window.GM_PHASE) window.GM_PHASE("LOG");
                window.setTimeout(function () {
                    if (window.requestAnimationFrame) {
                        window.requestAnimationFrame(function () {
                            window.requestAnimationFrame(removeOverlayNow);
                        });
                    } else {
                        window.setTimeout(removeOverlayNow, 16);
                    }
                }, 190);
            }

            window.GM_LOADER_SHOW = gmShowLoader;
            window.GM_PHASE = gmSetLoaderPhase;
            window.GM_LOADER_DONE = gmHideLoader;
            window.gmSubmitFormWithLoader = function (formEl, phaseCode) {
                function doSubmit() {
                    try {
                        if (typeof HTMLFormElement !== "undefined" && HTMLFormElement.prototype && HTMLFormElement.prototype.submit) {
                            HTMLFormElement.prototype.submit.call(formEl);
                        } else if (formEl && formEl.submit) {
                            formEl.submit();
                        }
                    } catch (e) {
                        try { if (formEl && formEl.submit) formEl.submit(); } catch (e2) {}
                    }
                }

                if (!formEl) return false;
                if (window.gmUpdateClientContext) window.gmUpdateClientContext();
                try { if (window.sessionStorage) sessionStorage.setItem("gm_loader_pending", "1"); } catch (e4) {}
                if (window.GM_LOADER_SHOW) window.GM_LOADER_SHOW(phaseCode || "TECH");

                if (window.requestAnimationFrame) {
                    window.requestAnimationFrame(function () {
                        window.requestAnimationFrame(function () {
                            window.setTimeout(doSubmit, 24);
                        });
                    });
                } else {
                    window.setTimeout(doSubmit, 24);
                }
                return true;
            };

            function pad2(n) {
                return n < 10 ? "0" + n : String(n);
            }

            function formatIsoLocal(dt) {
                return dt.getFullYear() + "-" + pad2(dt.getMonth() + 1) + "-" + pad2(dt.getDate()) + " " + pad2(dt.getHours()) + ":" + pad2(dt.getMinutes()) + ":" + pad2(dt.getSeconds());
            }

            function formatDmyLocal(dt) {
                return pad2(dt.getDate()) + "/" + pad2(dt.getMonth() + 1) + "/" + dt.getFullYear() + " " + pad2(dt.getHours()) + ":" + pad2(dt.getMinutes()) + ":" + pad2(dt.getSeconds());
            }

            function parseFlexibleDateTime(text) {
                var t = String(text || "").trim();
                var m;

                m = t.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
                if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));

                m = t.match(/^(\d{2})\/(\d{2})\/(\d{4})[ T](\d{2}):(\d{2}):(\d{2})$/);
                if (m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]), Number(m[4]), Number(m[5]), Number(m[6]));

                return null;
            }

            function gmSafeText(v) {
                if (v === null || typeof v === "undefined") return "";
                return String(v);
            }

            function gmTryParseJson(text, fallbackValue) {
                var raw = String(text || "").trim();
                var decoded;
                if (!raw) return fallbackValue;
                try {
                    return JSON.parse(raw);
                } catch (e1) {}

                try {
                    decoded = raw
                        .replace(/&quot;/g, "\"")
                        .replace(/&#34;/g, "\"")
                        .replace(/&apos;/g, "'")
                        .replace(/&#39;/g, "'")
                        .replace(/&lt;/g, "<")
                        .replace(/&gt;/g, ">")
                        .replace(/&amp;/g, "&");
                    return JSON.parse(decoded);
                } catch (e2) {
                    return fallbackValue;
                }
            }

            function gmSetGraphMessage(text, isError) {
                var msg = gmSafeText(text).trim();
                if (!graphPlaceholder) return;

                if (msg.length === 0) {
                    graphPlaceholder.textContent = "";
                    graphPlaceholder.classList.add("is-hidden");
                    graphPlaceholder.classList.remove("is-error");
                    if (graphCanvas) graphCanvas.style.display = "";
                    return;
                }

                graphPlaceholder.textContent = msg;
                graphPlaceholder.classList.remove("is-hidden");
                if (graphCanvas) graphCanvas.style.display = "none";
                if (isError) {
                    graphPlaceholder.classList.add("is-error");
                } else {
                    graphPlaceholder.classList.remove("is-error");
                }
            }

            function gmGetRowValueCI(rowObj, keyName) {
                var keys, i, k, target;
                if (!rowObj || typeof rowObj !== "object") return "";
                target = gmSafeText(keyName).toLowerCase();
                if (!target) return "";
                if (Object.prototype.hasOwnProperty.call(rowObj, keyName)) return rowObj[keyName];

                keys = Object.keys(rowObj);
                for (i = 0; i < keys.length; i++) {
                    k = keys[i];
                    if (gmSafeText(k).toLowerCase() === target) return rowObj[k];
                }
                return "";
            }

            function gmParseNumberLoose(v) {
                var t = gmSafeText(v).trim();
                var commaPos, dotPos, n;
                if (!t) return 0;

                t = t.replace(/[^0-9,.\-]/g, "");
                commaPos = t.lastIndexOf(",");
                dotPos = t.lastIndexOf(".");

                if (commaPos >= 0 && dotPos >= 0) {
                    if (commaPos > dotPos) {
                        t = t.replace(/\./g, "").replace(",", ".");
                    } else {
                        t = t.replace(/,/g, "");
                    }
                } else if (commaPos >= 0 && dotPos < 0) {
                    t = t.replace(",", ".");
                }

                n = parseFloat(t);
                return isNaN(n) ? 0 : n;
            }

            function gmToIntOrDefault(v, defaultValue) {
                var n = parseInt(v, 10);
                if (isNaN(n)) return defaultValue;
                return n;
            }

            function gmBuildTooltipFormatConfig(configObj) {
                var cfg = configObj || {};
                var tip = (cfg && typeof cfg.tooltip === "object") ? cfg.tooltip : {};
                var outCfg = {};

                outCfg.locale = gmSafeText(tip.locale || "en-US");
                if (!outCfg.locale) outCfg.locale = "en-US";

                outCfg.valueDecimals = gmToIntOrDefault(tip.valueDecimals, -1);
                if (outCfg.valueDecimals < -1) outCfg.valueDecimals = -1;

                outCfg.amountDecimals = gmToIntOrDefault(tip.amountDecimals, 2);
                if (outCfg.amountDecimals < 0) outCfg.amountDecimals = 2;

                outCfg.amountCurrency = gmSafeText(tip.amountCurrency || "");
                outCfg.amountCurrencyDisplay = gmSafeText(tip.amountCurrencyDisplay || "narrowSymbol");
                outCfg.amountPrefix = gmSafeText(tip.amountPrefix || "");
                if (!outCfg.amountPrefix && !outCfg.amountCurrency) outCfg.amountPrefix = "$";

                return outCfg;
            }

            function gmFormatTooltipValue(v, formatCfg) {
                var n = gmParseNumberLoose(v);
                var cfg = formatCfg || {};
                var locale = gmSafeText(cfg.locale || "en-US");
                var decimals = gmToIntOrDefault(cfg.valueDecimals, -1);

                if (!isFinite(n)) n = 0;
                if (decimals >= 0) {
                    return n.toLocaleString(locale, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
                }
                if (Math.abs(n - Math.round(n)) < 0.000001) return String(Math.round(n));
                return n.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 2 });
            }

            function gmFormatMoney(v, formatCfg) {
                var n = gmParseNumberLoose(v);
                var cfg = formatCfg || {};
                var locale = gmSafeText(cfg.locale || "en-US");
                var amountDecimals = gmToIntOrDefault(cfg.amountDecimals, 2);
                var amountCurrency = gmSafeText(cfg.amountCurrency || "");
                var amountCurrencyDisplay = gmSafeText(cfg.amountCurrencyDisplay || "narrowSymbol");
                var amountPrefix = gmSafeText(cfg.amountPrefix || "$");

                if (!isFinite(n)) n = 0;
                if (amountDecimals < 0) amountDecimals = 2;

                if (amountCurrency) {
                    try {
                        return new Intl.NumberFormat(locale, {
                            style: "currency",
                            currency: amountCurrency,
                            currencyDisplay: amountCurrencyDisplay,
                            minimumFractionDigits: amountDecimals,
                            maximumFractionDigits: amountDecimals
                        }).format(n);
                    } catch (e) {}
                }

                return amountPrefix + n.toLocaleString(locale, { minimumFractionDigits: amountDecimals, maximumFractionDigits: amountDecimals });
            }

            function gmNormalizeSeriesKey(v) {
                var t = gmSafeText(v).trim().toLowerCase();
                if (!t) return "";
                if (typeof t.normalize === "function") {
                    try {
                        t = t.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                    } catch (e) {}
                }
                return t;
            }

            function gmResolveSeriesColor(seriesName, seriesStyle, fallbackColor) {
                var k, target;
                if (!seriesStyle || typeof seriesStyle !== "object") return fallbackColor;
                if (Object.prototype.hasOwnProperty.call(seriesStyle, seriesName)) {
                    return gmSafeText(seriesStyle[seriesName]) || fallbackColor;
                }
                target = gmNormalizeSeriesKey(seriesName);
                for (k in seriesStyle) {
                    if (!Object.prototype.hasOwnProperty.call(seriesStyle, k)) continue;
                    if (gmNormalizeSeriesKey(k) === target) return gmSafeText(seriesStyle[k]) || fallbackColor;
                }
                return fallbackColor;
            }

            function gmFindColumnCI(cols, keyName) {
                var i, target;
                if (!Array.isArray(cols) || !cols.length) return "";
                target = gmSafeText(keyName).toLowerCase();
                if (!target) return "";
                for (i = 0; i < cols.length; i++) {
                    if (gmSafeText(cols[i]).toLowerCase() === target) return gmSafeText(cols[i]);
                }
                return "";
            }

            function gmPickColumnByCandidates(cols, candidates) {
                var i, colName;
                if (!Array.isArray(cols) || !cols.length || !Array.isArray(candidates)) return "";
                for (i = 0; i < candidates.length; i++) {
                    colName = gmFindColumnCI(cols, candidates[i]);
                    if (colName) return colName;
                }
                return "";
            }

            function gmGetGraphMapping(rows, configObj) {
                var cfg = configObj || {};
                var map = cfg.mapping || {};
                var cols = gmGetConfigColumns(rows);
                var labelKey = gmSafeText(map.label || map.labels || map.x || map.labelTitle || map.labelField);
                var valueKey = gmSafeText(map.value || map.values || map.y || map.valueField || map.value1 || map.amountField);
                var amountKey = gmSafeText(map.amount || map.amounts || map.amountField || map.value2 || map.monto);
                var seriesKey = gmSafeText(map.series || map.group || map.seriesTitle || map.seriesField);

                if (cols.length > 0) {
                    if (labelKey) labelKey = gmFindColumnCI(cols, labelKey);
                    if (valueKey) valueKey = gmFindColumnCI(cols, valueKey);
                    if (amountKey) amountKey = gmFindColumnCI(cols, amountKey);
                    if (seriesKey) seriesKey = gmFindColumnCI(cols, seriesKey);

                    if (!labelKey) labelKey = gmPickColumnByCandidates(cols, ["LabelTitle", "GraphLabel", "Label", "Estatus", "Status", "Categoria", "Category"]);
                    if (!valueKey) valueKey = gmPickColumnByCandidates(cols, ["Value1", "GraphValue", "Value", "Count", "Conteo", "Total", "Monto"]);
                    if (!amountKey) amountKey = gmPickColumnByCandidates(cols, ["Value2", "Amount", "GraphAmount", "Monto", "Importe"]);
                    if (!seriesKey) seriesKey = gmPickColumnByCandidates(cols, ["SeriesTitle", "Serie", "Series", "Grupo", "Group"]);

                    if (!labelKey && cols.length > 0) labelKey = cols[0];
                    if (!valueKey && cols.length > 1) valueKey = cols[1];
                    if (!amountKey && cols.length > 2) amountKey = cols[2];
                    if (!seriesKey && cols.length > 2) seriesKey = cols[2];
                }

                return {
                    label: labelKey,
                    value: valueKey,
                    amount: amountKey,
                    series: seriesKey
                };
            }

            function gmLoadChartJs(done) {
                var scriptEl;
                if (window.Chart) {
                    done(true);
                    return;
                }

                chartJsWaitQueue.push(done);
                if (chartJsIsLoading) return;
                chartJsIsLoading = true;

                scriptEl = document.createElement("script");
                scriptEl.src = "https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js";
                scriptEl.async = true;
                scriptEl.onload = function () {
                    var i;
                    chartJsIsLoading = false;
                    for (i = 0; i < chartJsWaitQueue.length; i++) chartJsWaitQueue[i](!!window.Chart);
                    chartJsWaitQueue = [];
                };
                scriptEl.onerror = function () {
                    var i;
                    chartJsIsLoading = false;
                    for (i = 0; i < chartJsWaitQueue.length; i++) chartJsWaitQueue[i](false);
                    chartJsWaitQueue = [];
                };
                document.head.appendChild(scriptEl);
            }

            function gmBuildGraphDatasets(rows, configObj) {
                var cfg = configObj || {};
                var mapping = gmGetGraphMapping(rows, cfg);
                var chartCfg = (cfg && typeof cfg.chart === "object") ? cfg.chart : {};
                var chartType = gmSafeText(cfg.chartType || chartCfg.type || "bar").toLowerCase();
                var forceSingle = (chartType === "pie" || chartType === "doughnut");
                var labels = [];
                var labelPos = {};
                var totals = {};
                var totalsAmount = {};
                var seriesData = {};
                var seriesAmountData = {};
                var seriesOrder = [];
                var i, rowObj, labelText, valueNum, amountNum, seriesName, idx, k;
                var valuesList, amountList, colorList;
                var datasets = [];
                var palette = Array.isArray(cfg.palette) && cfg.palette.length ? cfg.palette : ["#9085E9", "#E985AC", "#DEE985", "#85E9C2", "#D0CDEA"];
                var seriesStyle = (cfg && typeof cfg.seriesStyle === "object") ? cfg.seriesStyle : {};
                var datasetLabel = gmSafeText(cfg.datasetLabel || "Serie 1");
                var tooltipCfg = (cfg && typeof cfg.tooltip === "object") ? cfg.tooltip : {};
                var showAmountInTooltip = (typeof tooltipCfg.showAmount === "boolean") ? tooltipCfg.showAmount : true;
                var hasAmountField = !!mapping.amount;
                var cols;
                var backendMessage;
                var seriesColor;

                if (!Array.isArray(rows) || rows.length === 0) {
                    return { ok: false, message: gmMsgStdGraphNoData };
                }

                cols = gmGetConfigColumns(rows);
                if (cols.length === 1 && gmSafeText(cols[0]).toLowerCase() === "mensaje") {
                    backendMessage = gmSafeText(gmGetRowValueCI(rows[0], cols[0])).trim();
                    if (!backendMessage) backendMessage = gmMsgStdGraphSqlBuildFailed;
                    return { ok: false, message: backendMessage, isError: true };
                }

                if (!mapping.label || !mapping.value) {
                    return { ok: false, message: "Config de grafica invalida: falta mapping.label o mapping.value.", isError: true };
                }

                for (i = 0; i < rows.length; i++) {
                    rowObj = rows[i];
                    labelText = gmSafeText(gmGetRowValueCI(rowObj, mapping.label)).trim();
                    if (!labelText) labelText = "(Sin etiqueta)";
                    valueNum = gmParseNumberLoose(gmGetRowValueCI(rowObj, mapping.value));
                    amountNum = hasAmountField ? gmParseNumberLoose(gmGetRowValueCI(rowObj, mapping.amount)) : 0;

                    if (!Object.prototype.hasOwnProperty.call(labelPos, labelText)) {
                        labelPos[labelText] = labels.length;
                        labels.push(labelText);
                        for (k = 0; k < seriesOrder.length; k++) {
                            seriesData[seriesOrder[k]].push(0);
                            seriesAmountData[seriesOrder[k]].push(0);
                        }
                    }
                    idx = labelPos[labelText];

                    if (forceSingle || !mapping.series) {
                        if (!Object.prototype.hasOwnProperty.call(totals, labelText)) totals[labelText] = 0;
                        if (!Object.prototype.hasOwnProperty.call(totalsAmount, labelText)) totalsAmount[labelText] = 0;
                        totals[labelText] += valueNum;
                        totalsAmount[labelText] += amountNum;
                    } else {
                        seriesName = gmSafeText(gmGetRowValueCI(rowObj, mapping.series)).trim();
                        if (!seriesName) seriesName = "Serie";
                        if (!Object.prototype.hasOwnProperty.call(seriesData, seriesName)) {
                            seriesOrder.push(seriesName);
                            seriesData[seriesName] = [];
                            seriesAmountData[seriesName] = [];
                            for (k = 0; k < labels.length; k++) seriesData[seriesName].push(0);
                            for (k = 0; k < labels.length; k++) seriesAmountData[seriesName].push(0);
                        }
                        seriesData[seriesName][idx] += valueNum;
                        seriesAmountData[seriesName][idx] += amountNum;
                    }
                }

                if (!labels.length) {
                    return { ok: false, message: gmMsgStdGraphNoData };
                }

                if (forceSingle || !mapping.series) {
                    valuesList = [];
                    amountList = [];
                    colorList = [];
                    for (i = 0; i < labels.length; i++) {
                        valuesList.push(totals[labels[i]] || 0);
                        amountList.push(totalsAmount[labels[i]] || 0);
                        colorList.push(palette[i % palette.length]);
                    }
                    datasets.push({
                        label: datasetLabel,
                        data: valuesList,
                        gmAmounts: amountList,
                        gmShowAmounts: (showAmountInTooltip && hasAmountField),
                        borderColor: gmResolveSeriesColor(datasetLabel, seriesStyle, palette[0]),
                        backgroundColor: (chartType === "line") ? "rgba(53, 0, 114, 0.14)" : colorList
                    });
                } else {
                    for (i = 0; i < seriesOrder.length; i++) {
                        seriesName = seriesOrder[i];
                        seriesColor = gmResolveSeriesColor(seriesName, seriesStyle, palette[i % palette.length]);
                        datasets.push({
                            label: seriesName,
                            data: seriesData[seriesName],
                            gmAmounts: seriesAmountData[seriesName],
                            gmShowAmounts: (showAmountInTooltip && hasAmountField),
                            borderColor: seriesColor,
                            backgroundColor: seriesColor
                        });
                    }
                }

                return {
                    ok: true,
                    chartType: chartType,
                    labels: labels,
                    datasets: datasets
                };
            }

            function gmRenderMainGraph() {
                var rawRows, rawConfig, rows, configObj;
                var payload, ctx, optionsCfg, chartCfg, axesCfg, tooltipFormatCfg, animationCfg, legendEnabled, stackedEnabled;
                var responsiveEnabled, maintainAspectRatioEnabled, indexAxisValue;
                var yBeginAtZero, legendPosition, chartFontFamily, chartTextColor;
                var animationEnabled, animationDuration, animationEasing, animationDataDelay, animationDatasetDelay, animationBaseDelay;

                if (!graphCanvas || !graphDataNode || !graphConfigNode) return;

                rawRows = graphDataNode.textContent || "[]";
                rawConfig = graphConfigNode.textContent || "{}";
                rows = gmTryParseJson(rawRows, []);
                configObj = gmTryParseJson(rawConfig, {});

                if (!Array.isArray(rows)) rows = [];
                payload = gmBuildGraphDatasets(rows, configObj);
                if (!payload.ok) {
                    gmSetGraphMessage(payload.message, !!payload.isError);
                    return;
                }

                gmLoadChartJs(function (isReady) {
                    var chartOptions;
                    if (!isReady || !window.Chart) {
                        gmSetGraphMessage(gmMsgStdGraphLibLoadFailed, true);
                        return;
                    }

                    gmSetGraphMessage("", false);
                    ctx = graphCanvas.getContext("2d");
                    if (!ctx) {
                        gmSetGraphMessage(gmMsgStdGraphCanvasInitFailed, true);
                        return;
                    }

                    if (graphChartInstance && graphChartInstance.destroy) {
                        graphChartInstance.destroy();
                        graphChartInstance = null;
                    }

                    optionsCfg = configObj && configObj.options ? configObj.options : {};
                    chartCfg = (configObj && typeof configObj.chart === "object") ? configObj.chart : {};
                    axesCfg = (configObj && typeof configObj.axes === "object") ? configObj.axes : {};
                    animationCfg = (configObj && typeof configObj.animation === "object") ? configObj.animation : {};
                    tooltipFormatCfg = gmBuildTooltipFormatConfig(configObj);
                    legendEnabled = (typeof optionsCfg.legend === "boolean")
                        ? optionsCfg.legend
                        : ((typeof chartCfg.legend === "boolean") ? chartCfg.legend : true);
                    stackedEnabled = (typeof optionsCfg.stacked === "boolean")
                        ? optionsCfg.stacked
                        : ((typeof chartCfg.stacked === "boolean") ? chartCfg.stacked : false);
                    responsiveEnabled = (typeof chartCfg.responsive === "boolean") ? chartCfg.responsive : true;
                    maintainAspectRatioEnabled = (typeof chartCfg.maintainAspectRatio === "boolean") ? chartCfg.maintainAspectRatio : false;
                    yBeginAtZero = (typeof axesCfg.yBeginAtZero === "boolean") ? axesCfg.yBeginAtZero : true;
                    legendPosition = gmSafeText(optionsCfg.legendPosition || chartCfg.legendPosition || "bottom").toLowerCase();
                    if (legendPosition !== "top" && legendPosition !== "left" && legendPosition !== "right" && legendPosition !== "bottom") legendPosition = "bottom";
                    indexAxisValue = gmSafeText(optionsCfg.indexAxis || chartCfg.indexAxis).toLowerCase();
                    chartFontFamily = "\"DM Sans\", \"Open Sans\", Arial, sans-serif";
                    chartTextColor = "#6b7280";
                    animationEnabled = (typeof animationCfg.enabled === "boolean") ? animationCfg.enabled : true;
                    animationDuration = gmToIntOrDefault(animationCfg.duration, 900);
                    if (animationDuration < 0) animationDuration = 900;
                    animationEasing = gmSafeText(animationCfg.easing || "easeOutQuart");
                    if (!animationEasing) animationEasing = "easeOutQuart";
                    animationDataDelay = gmToIntOrDefault(animationCfg.dataDelay, 90);
                    if (animationDataDelay < 0) animationDataDelay = 0;
                    animationDatasetDelay = gmToIntOrDefault(animationCfg.datasetDelay, 120);
                    if (animationDatasetDelay < 0) animationDatasetDelay = 0;
                    animationBaseDelay = gmToIntOrDefault(animationCfg.baseDelay, 0);
                    if (animationBaseDelay < 0) animationBaseDelay = 0;

                    chartOptions = {
                        responsive: responsiveEnabled,
                        maintainAspectRatio: maintainAspectRatioEnabled,
                        animation: animationEnabled ? {
                            duration: animationDuration,
                            easing: animationEasing,
                            delay: function (context) {
                                if (context && context.type === "data") {
                                    return animationBaseDelay + (context.dataIndex * animationDataDelay) + (context.datasetIndex * animationDatasetDelay);
                                }
                                return animationBaseDelay;
                            }
                        } : false,
                        plugins: {
                            legend: {
                                display: legendEnabled,
                                position: legendPosition,
                                labels: {
                                    color: chartTextColor,
                                    font: {
                                        family: chartFontFamily,
                                        size: 12,
                                        weight: "400"
                                    }
                                }
                            },
                            tooltip: {
                                callbacks: {
                                    title: function (items) {
                                        if (!items || !items.length) return "";
                                        return gmSafeText(items[0].label);
                                    },
                                    label: function (context) {
                                        var seriesName = gmSafeText(context && context.dataset ? context.dataset.label : "Serie");
                                        var rawValue = 0;
                                        if (context && typeof context.parsed === "number") {
                                            rawValue = context.parsed;
                                        } else if (context && context.parsed && typeof context.parsed.y !== "undefined") {
                                            rawValue = context.parsed.y;
                                        } else if (context && context.parsed && typeof context.parsed.x !== "undefined") {
                                            rawValue = context.parsed.x;
                                        }
                                        return seriesName + ": " + gmFormatTooltipValue(rawValue, tooltipFormatCfg);
                                    },
                                    afterLabel: function (context) {
                                        var ds, amounts, idx;
                                        ds = context && context.dataset ? context.dataset : null;
                                        if (!ds) return "";
                                        if (ds.gmShowAmounts !== true) return "";
                                        amounts = ds.gmAmounts;
                                        idx = context ? context.dataIndex : -1;
                                        if (!Array.isArray(amounts) || idx < 0 || idx >= amounts.length) return "";
                                        return gmFormatMoney(amounts[idx], tooltipFormatCfg);
                                    }
                                }
                            }
                        }
                    };

                    if (payload.chartType !== "pie" && payload.chartType !== "doughnut") {
                        chartOptions.scales = {
                            x: {
                                stacked: stackedEnabled,
                                ticks: {
                                    color: chartTextColor,
                                    font: {
                                        family: chartFontFamily,
                                        size: 12,
                                        weight: "400"
                                    }
                                }
                            },
                            y: {
                                stacked: stackedEnabled,
                                beginAtZero: yBeginAtZero,
                                ticks: {
                                    color: chartTextColor,
                                    font: {
                                        family: chartFontFamily,
                                        size: 12,
                                        weight: "400"
                                    }
                                }
                            }
                        };
                    }

                    if (indexAxisValue === "y") {
                        chartOptions.indexAxis = "y";
                    }

                    graphChartInstance = new Chart(ctx, {
                        type: payload.chartType,
                        data: {
                            labels: payload.labels,
                            datasets: payload.datasets
                        },
                        options: chartOptions
                    });
                });
            }

            function gmConfigValueToText(v) {
                if (v === null) return "null";
                if (typeof v === "undefined") return "";
                if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
                try {
                    return JSON.stringify(v);
                } catch (e) {
                    return String(v);
                }
            }

            function gmGetConfigColumns(rows) {
                var cols = [];
                var seen = {};
                var i, k, row;

                for (i = 0; i < rows.length; i++) {
                    row = rows[i];
                    if (!row || typeof row !== "object") continue;
                    for (k in row) {
                        if (Object.prototype.hasOwnProperty.call(row, k) && !seen[k]) {
                            seen[k] = true;
                            cols.push(k);
                        }
                    }
                }
                return cols;
            }

            function gmApplyConfigStickyColumns(tableEl, freezeCount) {
                var headRow, maxFreeze, leftOffset, colIndex, rowIndex, rowCells, cell, colWidth;
                if (!tableEl || !tableEl.tHead || !tableEl.tHead.rows.length) return;

                headRow = tableEl.tHead.rows[0];
                maxFreeze = Math.min(freezeCount, headRow.cells.length);

                leftOffset = 0;
                for (colIndex = 0; colIndex < maxFreeze; colIndex++) {
                    colWidth = Math.ceil(headRow.cells[colIndex].getBoundingClientRect().width);
                    for (rowIndex = 0; rowIndex < tableEl.rows.length; rowIndex++) {
                        rowCells = tableEl.rows[rowIndex].cells;
                        if (!rowCells || rowCells.length <= colIndex) continue;
                        cell = rowCells[colIndex];
                        cell.classList.add("gm-config-freeze-col");
                        cell.style.left = leftOffset + "px";
                    }
                    leftOffset = leftOffset + colWidth;
                }
            }

            function gmRenderConfigFallback(text) {
                var pre;
                if (!configModalContent) return;
                configRawContent = gmSafeText(text);
                configModalContent.innerHTML = "";
                pre = document.createElement("pre");
                pre.className = "gm-config-modal-fallback";
                pre.textContent = configRawContent;
                configModalContent.appendChild(pre);
            }

            function gmRenderConfigTable(text) {
                var parsed, rows, columns, table, thead, tbody, trh, th, tr, td;
                var i, j, rowObj, colName;

                configRawContent = gmSafeText(text);
                if (!configModalContent) return;

                if (configStickyRaf) {
                    window.cancelAnimationFrame(configStickyRaf);
                    configStickyRaf = null;
                }

                if (configRawContent.trim().length === 0) {
                    configModalContent.innerHTML = "";
                    return;
                }

                try {
                    parsed = JSON.parse(configRawContent);
                } catch (e) {
                    gmRenderConfigFallback(configRawContent);
                    return;
                }

                if (!Array.isArray(parsed)) {
                    gmRenderConfigFallback(configRawContent);
                    return;
                }

                rows = parsed;
                if (!rows.length) {
                    gmRenderConfigFallback("[]");
                    return;
                }

                columns = gmGetConfigColumns(rows);
                if (!columns.length) {
                    gmRenderConfigFallback(configRawContent);
                    return;
                }

                table = document.createElement("table");
                table.className = "gm-config-table";

                thead = document.createElement("thead");
                trh = document.createElement("tr");
                for (i = 0; i < columns.length; i++) {
                    th = document.createElement("th");
                    th.textContent = gmSafeText(columns[i]);
                    trh.appendChild(th);
                }
                thead.appendChild(trh);
                table.appendChild(thead);

                tbody = document.createElement("tbody");
                for (i = 0; i < rows.length; i++) {
                    rowObj = rows[i];
                    tr = document.createElement("tr");
                    for (j = 0; j < columns.length; j++) {
                        colName = columns[j];
                        td = document.createElement("td");
                        td.textContent = gmConfigValueToText(rowObj && typeof rowObj === "object" ? rowObj[colName] : "");
                        tr.appendChild(td);
                    }
                    tbody.appendChild(tr);
                }
                table.appendChild(tbody);

                configModalContent.innerHTML = "";
                configModalContent.appendChild(table);

                configStickyRaf = window.requestAnimationFrame(function () {
                    gmApplyConfigStickyColumns(table, 4);
                    configStickyRaf = null;
                });
            }

            function gmRefreshConfigSticky() {
                var table;
                if (!configModalContent) return;
                table = configModalContent.querySelector(".gm-config-table");
                if (!table) return;

                if (configStickyRaf) {
                    window.cancelAnimationFrame(configStickyRaf);
                    configStickyRaf = null;
                }
                configStickyRaf = window.requestAnimationFrame(function () {
                    gmApplyConfigStickyColumns(table, 4);
                    configStickyRaf = null;
                });
            }

            function gmGetConfigTableCopyText() {
                var table, rows, lines, i, j, cells, lineParts, cellText;
                table = configModalContent ? configModalContent.querySelector(".gm-config-table") : null;
                if (!table) return gmSafeText(configRawContent);

                rows = table.querySelectorAll("tr");
                lines = [];
                for (i = 0; i < rows.length; i++) {
                    cells = rows[i].querySelectorAll("th, td");
                    if (!cells || !cells.length) continue;
                    lineParts = [];
                    for (j = 0; j < cells.length; j++) {
                        cellText = gmSafeText(cells[j].textContent);
                        cellText = cellText.replace(/\r\n/g, " ").replace(/\n/g, " ").replace(/\t/g, " ").trim();
                        lineParts.push(cellText);
                    }
                    lines.push(lineParts.join("\t"));
                }
                return lines.join("\n");
            }

            function replaceContractLine(prefix, newLine) {
                var lines, i;
                if (!metaContract) return;
                lines = (metaContract.textContent || "").replace(/\r/g, "").split("\n");
                for (i = 0; i < lines.length; i++) {
                    if (lines[i].indexOf(prefix) === 0) {
                        lines[i] = newLine;
                        metaContract.textContent = lines.join("\n");
                        return;
                    }
                }
            }

            function syncSupportLinkFromContract() {
                var metaText, waMsg;
                if (!waSoporte || !metaContract) return;
                metaText = (metaContract.innerText || metaContract.textContent || "").replace(/\r/g, "").trim();
                if (metaText.length === 0) return;
                waMsg = gmWaSupportTemplate || "Hola! Necesito soporte - {MetadataText}";
                waMsg = waMsg.replace("{MetadataText}", metaText);
                waSoporte.href = "https://wa.me/<%=SupportWA%>?text=" + encodeURIComponent(waMsg);
            }

            function setCtxHiddenValue(id, value) {
                var el = document.getElementById(id);
                if (el) el.value = value;
            }

            function updateClientContextFields() {
                var nowLocalDate = new Date();
                var nowLocalText = formatIsoLocal(nowLocalDate);
                var tzName = "Local";
                var tzOffsetMin = -nowLocalDate.getTimezoneOffset();

                try {
                    tzName = Intl.DateTimeFormat().resolvedOptions().timeZone || "Local";
                } catch (e) {}

                setCtxHiddenValue("gm-topbar-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-topbar-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-topbar-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-menu-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-menu-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-menu-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-filter-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-filter-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-filter-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-export-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-export-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-export-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-config-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-config-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-config-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-gconfig-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-gconfig-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-gconfig-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-sql-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-sql-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-sql-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-filters-sql-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-filters-sql-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-filters-sql-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-kpis-sql-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-kpis-sql-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-kpis-sql-ctx-client-offset", String(tzOffsetMin));

                setCtxHiddenValue("gm-graph-sql-ctx-client-now", nowLocalText);
                setCtxHiddenValue("gm-graph-sql-ctx-client-tz", tzName);
                setCtxHiddenValue("gm-graph-sql-ctx-client-offset", String(tzOffsetMin));

                replaceContractLine("- TimeZone:", "- TimeZone: " + tzName);
                replaceContractLine("- Timestamp:", "- Timestamp: " + nowLocalText);
                syncSupportLinkFromContract();
            }

            window.gmUpdateClientContext = updateClientContextFields;

            function isAnyPopupModalOpen() {
                var isSqlOpen = !!(sqlModal && sqlModal.classList.contains("is-open"));
                var isConfigOpen = !!(configModal && configModal.classList.contains("is-open"));
                return isSqlOpen || isConfigOpen;
            }

            function openPopupModal(modalEl) {
                if (!modalEl) return;
                modalEl.classList.add("is-open");
                if (document.body) document.body.classList.add("gm-sql-modal-open");
            }

            function closePopupModal(modalEl) {
                if (!modalEl) return;
                modalEl.classList.remove("is-open");
                if (document.body && !isAnyPopupModalOpen()) document.body.classList.remove("gm-sql-modal-open");
            }

            function openConfigModal() {
                closePopupModal(sqlModal);
                openPopupModal(configModal);
                gmRefreshConfigSticky();
            }

            function closeConfigModal() {
                closePopupModal(configModal);
            }

            function openSqlModal() {
                closePopupModal(configModal);
                openPopupModal(sqlModal);
            }

            function closeSqlModal() {
                closePopupModal(sqlModal);
            }

            function fallbackCopyText(text) {
                var ta;
                try {
                    ta = document.createElement("textarea");
                    ta.value = String(text || "");
                    ta.setAttribute("readonly", "readonly");
                    ta.style.position = "fixed";
                    ta.style.left = "-9999px";
                    document.body.appendChild(ta);
                    ta.select();
                    ta.setSelectionRange(0, ta.value.length);
                    if (document.execCommand("copy")) {
                        document.body.removeChild(ta);
                        return true;
                    }
                    document.body.removeChild(ta);
                } catch (e) {}
                return false;
            }

            function setCopyButtonState(buttonEl, ok) {
                var original;
                if (!buttonEl) return;
                original = buttonEl.getAttribute("data-label-original");
                if (!original) {
                    original = buttonEl.textContent || "Copy";
                    buttonEl.setAttribute("data-label-original", original);
                }
                buttonEl.textContent = ok ? "Copied" : "Copy failed";
                window.setTimeout(function () {
                    buttonEl.textContent = original;
                }, 1200);
            }

            function getCurrentFilterValue(level) {
                var selectEl = document.getElementById("gm-filter" + level + "-select");
                var hiddenEl;
                if (selectEl) return selectEl.value || "";
                hiddenEl = document.querySelector(".gm-main-report-filter-form input[name='filter" + level + "']");
                if (hiddenEl) return hiddenEl.value || "";
                return "";
            }

            function syncPopupFormValues(prefix) {
                var sourceUser = document.getElementById("topbar_user_filter");
                var sourceReport = document.getElementById("report");
                var sourceClinic = document.getElementById("clinic");
                var sourceStart = document.getElementById("start_date");
                var sourceEnd = document.getElementById("end_date");
                var sourceFilter1 = getCurrentFilterValue(1);
                var sourceFilter2 = getCurrentFilterValue(2);
                var sourceFilter3 = getCurrentFilterValue(3);
                var targetUser = document.getElementById(prefix + "-user-filter");
                var targetReport = document.getElementById(prefix + "-report");
                var targetClinic = document.getElementById(prefix + "-clinic");
                var targetStart = document.getElementById(prefix + "-start-date");
                var targetEnd = document.getElementById(prefix + "-end-date");
                var targetFilter1 = document.getElementById(prefix + "-filter1");
                var targetFilter2 = document.getElementById(prefix + "-filter2");
                var targetFilter3 = document.getElementById(prefix + "-filter3");

                updateClientContextFields();
                if (targetUser && sourceUser) targetUser.value = sourceUser.value;
                if (targetReport && sourceReport) targetReport.value = sourceReport.value;
                if (targetClinic && sourceClinic) targetClinic.value = sourceClinic.value;
                if (targetStart && sourceStart) targetStart.value = sourceStart.value;
                if (targetEnd && sourceEnd) targetEnd.value = sourceEnd.value;
                if (targetFilter1) targetFilter1.value = sourceFilter1;
                if (targetFilter2) targetFilter2.value = sourceFilter2;
                if (targetFilter3) targetFilter3.value = sourceFilter3;
            }

            function syncSqlPopupFormValues() { syncPopupFormValues("gm-sql"); }
            function syncFiltersSqlPopupFormValues() { syncPopupFormValues("gm-filters-sql"); }
            function syncKpisSqlPopupFormValues() { syncPopupFormValues("gm-kpis-sql"); }
            function syncGraphSqlPopupFormValues() { syncPopupFormValues("gm-graph-sql"); }
            function syncConfigPopupFormValues() { syncPopupFormValues("gm-config"); }
            function syncGConfigPopupFormValues() { syncPopupFormValues("gm-gconfig"); }

            function buildPopupRequestBody(formEl, apiFlagName) {
                var fields, i, parts, fieldName, fieldValue;
                if (!formEl || !formEl.querySelectorAll) return "";

                fields = formEl.querySelectorAll("input[name]");
                parts = [];
                for (i = 0; i < fields.length; i++) {
                    fieldName = fields[i].name || "";
                    if (!fieldName) continue;
                    fieldValue = fields[i].value || "";
                    parts.push(encodeURIComponent(fieldName) + "=" + encodeURIComponent(fieldValue));
                }
                parts.push(encodeURIComponent(apiFlagName) + "=1");
                return parts.join("&");
            }

            function requestSqlPopupContentByForm(formEl, syncFn) {
                var xhr, body;

                if (!formEl) return;
                if (typeof syncFn === "function") syncFn();
                body = buildPopupRequestBody(formEl, "sql_popup_api");
                if (!body) return;

                if (sqlModalTitle) sqlModalTitle.textContent = "<%=GmJsonEscape(CStr(MsgDevSqlLoading))%>";
                if (sqlModalContent) sqlModalContent.textContent = "";
                openSqlModal();

                xhr = new XMLHttpRequest();
                xhr.open("POST", window.location.pathname, true);
                xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
                xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
                xhr.onreadystatechange = function () {
                    var payload, prefix;

                    if (xhr.readyState !== 4) return;

                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            payload = JSON.parse(xhr.responseText || "{}");
                        } catch (e) {
                            payload = null;
                        }

                        if (payload && payload.ok === true) {
                            prefix = payload.isSafe ? "\u2705 " : "\u274C ";
                            if (sqlModalTitle) sqlModalTitle.textContent = prefix + String(payload.url || "SQL");
                            if (sqlModalContent) {
                                sqlModalContent.textContent = String(payload.sql || "<%=GmJsonEscape(CStr(MsgDevSqlEmptyRender))%>");
                            }
                            return;
                        }
                    }

                    if (sqlModalTitle) sqlModalTitle.textContent = "\u274C <%=GmJsonEscape(CStr(MsgDevSqlPopupErrorTitle))%>";
                    if (sqlModalContent) {
                        sqlModalContent.textContent = "<%=GmJsonEscape(CStr(MsgDevSqlPopupErrorBody))%>";
                    }
                };
                xhr.send(body);
            }

            function requestConfigPopupContentByForm(formEl, syncFn) {
                var xhr, body;

                if (!formEl) return;
                if (typeof syncFn === "function") syncFn();
                body = buildPopupRequestBody(formEl, "config_popup_api");
                if (!body) return;

                if (configModalTitle) configModalTitle.textContent = "<%=GmJsonEscape(CStr(MsgDevConfigLoading))%>";
                gmRenderConfigFallback("<%=GmJsonEscape(CStr(MsgDevConfigLoading))%>");
                openConfigModal();

                xhr = new XMLHttpRequest();
                xhr.open("POST", window.location.pathname, true);
                xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
                xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
                xhr.onreadystatechange = function () {
                    var payload, prefix, buildStatus, configPayloadText;

                    if (xhr.readyState !== 4) return;

                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            payload = JSON.parse(xhr.responseText || "{}");
                        } catch (e) {
                            payload = null;
                        }

                        if (payload && payload.ok === true) {
                            buildStatus = String(payload.buildStatus || "").toUpperCase();
                            if (buildStatus === "OK") {
                                prefix = "\u2705 ";
                            } else if (buildStatus === "EMPTY") {
                                prefix = "";
                            } else {
                                prefix = "\u274C ";
                            }
                            if (configModalTitle) configModalTitle.textContent = prefix + String(payload.url || "Config");
                            configPayloadText = "";
                            if (Object.prototype.hasOwnProperty.call(payload, "config")) {
                                if (payload.config !== null && typeof payload.config !== "undefined") {
                                    configPayloadText = String(payload.config);
                                }
                            }
                            gmRenderConfigTable(configPayloadText);
                            return;
                        }
                    }

                    if (configModalTitle) configModalTitle.textContent = "\u274C <%=GmJsonEscape(CStr(MsgDevConfigPopupErrorTitle))%>";
                    gmRenderConfigFallback("<%=GmJsonEscape(CStr(MsgDevConfigPopupErrorBody))%>");
                };
                xhr.send(body);
            }

            function requestSqlPopupContent() {
                requestSqlPopupContentByForm(sqlPopupForm, syncSqlPopupFormValues);
            }

            function requestFiltersSqlPopupContent() {
                requestSqlPopupContentByForm(filtersSqlPopupForm, syncFiltersSqlPopupFormValues);
            }

            function requestKpisSqlPopupContent() {
                requestSqlPopupContentByForm(kpisSqlPopupForm, syncKpisSqlPopupFormValues);
            }

            function requestGraphSqlPopupContent() {
                requestSqlPopupContentByForm(graphSqlPopupForm, syncGraphSqlPopupFormValues);
            }

            function requestConfigPopupContent() {
                requestConfigPopupContentByForm(configPopupForm, syncConfigPopupFormValues);
            }

            function requestConfigPopupContentByKind(kindValue) {
                var kindInput = document.getElementById("gm-config-popup-kind");
                if (kindInput) kindInput.value = gmSafeText(kindValue || "reports").toLowerCase();
                requestConfigPopupContent();
            }

            function requestGConfigPopupContent() {
                requestConfigPopupContentByForm(gConfigPopupForm, syncGConfigPopupFormValues);
            }

            function localizeContractDateTimes() {
                var lines, i, line, m;
                var tzName, nowLocalText, nowLocalDate;
                var sessionLineIndex, timestampLineIndex, timezoneLineIndex, sessionIDText;
                var sessionStartText, serverStampText;
                var sessionStartDate, serverStampDate, offsetMs, sessionLocalDate;
                var postedClientNowField, hasPostedClientNow;

                if (!metaContract) return;
                lines = (metaContract.textContent || "").replace(/\r/g, "").split("\n");
                if (!lines.length) return;

                tzName = "Local";
                try {
                    tzName = Intl.DateTimeFormat().resolvedOptions().timeZone || "Local";
                } catch (e) {}

                nowLocalDate = new Date();
                nowLocalText = formatIsoLocal(nowLocalDate);
                postedClientNowField = document.getElementById("gm-menu-ctx-client-now");
                hasPostedClientNow = !!(postedClientNowField && String(postedClientNowField.value || "").trim().length > 0);
                sessionLineIndex = -1;
                timestampLineIndex = -1;
                timezoneLineIndex = -1;
                sessionIDText = "";
                sessionStartText = "";
                serverStampText = "";

                for (i = 0; i < lines.length; i++) {
                    line = lines[i];
                    if (line.indexOf("- Sesion:") === 0) {
                        sessionLineIndex = i;
                        m = line.match(/^- Sesion:\s*(.*?)\s*-\s*(.+)$/);
                        if (m) {
                            sessionIDText = m[1];
                            sessionStartText = m[2];
                        }
                    } else if (line.indexOf("- Timestamp:") === 0) {
                        timestampLineIndex = i;
                        m = line.match(/^- Timestamp:\s*(.+?)\s*-\s*(.+)$/);
                        if (m) {
                            serverStampText = m[1];
                        } else {
                            serverStampText = line.replace(/^- Timestamp:\s*/, "");
                        }
                    } else if (line.indexOf("- TimeZone:") === 0) {
                        timezoneLineIndex = i;
                    }
                }

                if (timestampLineIndex >= 0) {
                    lines[timestampLineIndex] = "- Timestamp: " + nowLocalText;
                }
                if (timezoneLineIndex >= 0) {
                    lines[timezoneLineIndex] = "- TimeZone: " + tzName;
                }

                sessionStartDate = parseFlexibleDateTime(sessionStartText);
                serverStampDate = parseFlexibleDateTime(serverStampText);
                if (!hasPostedClientNow && sessionLineIndex >= 0 && sessionStartDate && serverStampDate) {
                    offsetMs = nowLocalDate.getTime() - serverStampDate.getTime();
                    sessionLocalDate = new Date(sessionStartDate.getTime() + offsetMs);
                    lines[sessionLineIndex] = "- Sesion: " + sessionIDText + " - " + formatDmyLocal(sessionLocalDate);
                }

                metaContract.textContent = lines.join("\n");
            }

            if (metaToggle) {
                metaToggle.addEventListener("click", function (e) {
                    e.preventDefault();
                    this.focus();
                });
            }
            if (fullScreenLink) {
                fullScreenLink.addEventListener("click", function (e) {
                    var el = document.documentElement;
                    e.preventDefault();
                    if (el.requestFullscreen) el.requestFullscreen();
                });
            }

            if (masterPopupLink && configPopupForm) {
                masterPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestConfigPopupContentByKind("master");
                });
            }

            if (modulesPopupLink && configPopupForm) {
                modulesPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestConfigPopupContentByKind("modules");
                });
            }

            if (homePopupLink && configPopupForm) {
                homePopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestConfigPopupContentByKind("home");
                });
            }

            if (configPopupLink && configPopupForm) {
                configPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestConfigPopupContentByKind("reports");
                });
            }

            if (sqlPopupLink && sqlPopupForm) {
                sqlPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestSqlPopupContent();
                });
            }

            if (filtersSqlPopupLink && filtersSqlPopupForm) {
                filtersSqlPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestFiltersSqlPopupContent();
                });
            }

            if (kpisSqlPopupLink && kpisSqlPopupForm) {
                kpisSqlPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestKpisSqlPopupContent();
                });
            }

            if (graphSqlPopupLink && graphSqlPopupForm) {
                graphSqlPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestGraphSqlPopupContent();
                });
            }

            if (gConfigPopupLink && gConfigPopupForm) {
                gConfigPopupLink.addEventListener("click", function (e) {
                    e.preventDefault();
                    requestGConfigPopupContent();
                });
            }

            if (configModalClose) {
                configModalClose.addEventListener("click", function () {
                    closeConfigModal();
                });
            }

            if (sqlModalClose) {
                sqlModalClose.addEventListener("click", function () {
                    closeSqlModal();
                });
            }

            if (configModalCopy) {
                configModalCopy.addEventListener("click", function () {
                    var configText = gmGetConfigTableCopyText();
                    if (String(configText || "").trim().length === 0) configText = gmSafeText(configRawContent);
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(configText).then(function () {
                            setCopyButtonState(configModalCopy, true);
                        }, function () {
                            setCopyButtonState(configModalCopy, fallbackCopyText(configText));
                        });
                    } else {
                        setCopyButtonState(configModalCopy, fallbackCopyText(configText));
                    }
                });
            }

            if (sqlModalCopy) {
                sqlModalCopy.addEventListener("click", function () {
                    var sqlContent = document.getElementById("gm-sql-modal-content");
                    var sqlText = sqlContent ? (sqlContent.textContent || "") : "";
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(sqlText).then(function () {
                            setCopyButtonState(sqlModalCopy, true);
                        }, function () {
                            setCopyButtonState(sqlModalCopy, fallbackCopyText(sqlText));
                        });
                    } else {
                        setCopyButtonState(sqlModalCopy, fallbackCopyText(sqlText));
                    }
                });
            }

            if (configModal) {
                configModal.addEventListener("click", function (e) {
                    if (e.target === configModal) closeConfigModal();
                });
            }

            if (sqlModal) {
                sqlModal.addEventListener("click", function (e) {
                    if (e.target === sqlModal) closeSqlModal();
                });
            }
            document.addEventListener("keydown", function (e) {
                if (e.key === "Escape") {
                    if (configModal && configModal.classList.contains("is-open")) closeConfigModal();
                    if (sqlModal && sqlModal.classList.contains("is-open")) closeSqlModal();
                }
            });

            var sel = document.getElementById("topbar_user_filter");
            var display = document.getElementById("gm-user-display");
            var dropdown = document.getElementById("gm-user-dropdown");
            var trigger = document.getElementById("gm-user-trigger");
            var menu = document.getElementById("gm-user-menu");

            function updateUserText() {
                if (!sel || !display) return;
                if (sel.selectedIndex < 0) {
                    display.textContent = "";
                    return;
                }
                display.textContent = sel.options[sel.selectedIndex].text;
            }

            function renderUserMenu() {
                var i, option, item;
                if (!sel || !menu) return;
                menu.innerHTML = "";

                for (i = 0; i < sel.options.length; i++) {
                    option = sel.options[i];
                    item = document.createElement("button");
                    item.type = "button";
                    item.className = "gm-topbar-user-option" + (option.selected ? " is-selected" : "");
                    item.setAttribute("role", "option");
                    item.setAttribute("aria-selected", option.selected ? "true" : "false");
                    item.textContent = option.text;
                    item.dataset.value = option.value;
                    item.addEventListener("click", function () {
                        if (!sel) return;
                        if (sel.value !== this.dataset.value) sel.value = this.dataset.value;
                        updateClientContextFields();
                        updateUserText();
                        closeUserMenu();
                        if (window.gmSubmitFormWithLoader) {
                            window.gmSubmitFormWithLoader(sel.form, "TECH");
                        } else if (sel.form && sel.form.submit) {
                            sel.form.submit();
                        }
                    });
                    menu.appendChild(item);
                }
            }

            function openUserMenu() {
                if (!dropdown || !trigger) return;
                renderUserMenu();
                dropdown.classList.add("is-open");
                trigger.setAttribute("aria-expanded", "true");
            }

            function closeUserMenu() {
                if (!dropdown || !trigger) return;
                dropdown.classList.remove("is-open");
                trigger.setAttribute("aria-expanded", "false");
            }

            function toggleUserMenu() {
                if (!dropdown) return;
                if (dropdown.classList.contains("is-open")) {
                    closeUserMenu();
                } else {
                    openUserMenu();
                }
            }

            if (sel && display && dropdown && trigger && menu) {
                updateUserText();
                renderUserMenu();

                trigger.addEventListener("mousedown", function (e) {
                    e.preventDefault();
                });

                trigger.addEventListener("click", function (e) {
                    e.preventDefault();
                    e.stopPropagation();
                    toggleUserMenu();
                });

                sel.addEventListener("change", function () {
                    updateUserText();
                    renderUserMenu();
                });

                document.addEventListener("click", function (e) {
                    if (!dropdown.contains(e.target)) closeUserMenu();
                });

                document.addEventListener("keydown", function (e) {
                    if (e.key === "Escape") closeUserMenu();
                });
            }

            (function bindClientContextOnSubmit() {
                var topbarForm = document.querySelector(".gm-topbar-userform");
                var menuForm = document.querySelector("form.gm-modal.menu");
                var filterForm = document.querySelector(".gm-main-report-filter-form");
                var exportForm = document.getElementById("gm-export-form");
                function bindForm(formEl, showLoader) {
                    if (!formEl) return;
                    formEl.addEventListener("submit", function (e) {
                        if (!showLoader) {
                            updateClientContextFields();
                            return;
                        }

                        if (e && e.preventDefault) e.preventDefault();
                        if (window.gmSubmitFormWithLoader) {
                            window.gmSubmitFormWithLoader(formEl, "TECH");
                        } else {
                            updateClientContextFields();
                            if (window.GM_LOADER_SHOW) window.GM_LOADER_SHOW("TECH");
                            window.setTimeout(function () {
                                try { formEl.submit(); } catch (e2) {}
                            }, 24);
                        }
                    });
                }

                bindForm(topbarForm, true);
                bindForm(menuForm, false);
                bindForm(filterForm, true);
                bindForm(exportForm, false);
            })();

            function initMenuSelect(config) {
                var nativeSelect = document.getElementById(config.selectId);
                var displayText = document.getElementById(config.displayId);
                var wrapper = document.getElementById(config.wrapperId);
                var triggerBtn = document.getElementById(config.triggerId);
                var optionMenu = document.getElementById(config.menuId);
                var submitOnPick = !!config.submitOnPick;

                function dispatchNativeChange() {
                    var evt;
                    if (!nativeSelect) return;

                    if (typeof Event === "function") {
                        nativeSelect.dispatchEvent(new Event("change", { bubbles: true }));
                        return;
                    }

                    if (document.createEvent) {
                        evt = document.createEvent("HTMLEvents");
                        evt.initEvent("change", true, false);
                        nativeSelect.dispatchEvent(evt);
                    }
                }

                function updateText() {
                    if (!nativeSelect || !displayText) return;
                    if (nativeSelect.selectedIndex < 0) {
                        displayText.textContent = "";
                        return;
                    }
                    displayText.textContent = nativeSelect.options[nativeSelect.selectedIndex].text;
                }

                function renderMenu() {
                    var i, option, item;
                    if (!nativeSelect || !optionMenu) return;
                    optionMenu.innerHTML = "";

                    for (i = 0; i < nativeSelect.options.length; i++) {
                        option = nativeSelect.options[i];
                        item = document.createElement("button");
                        item.type = "button";
                        item.className = "gm-select-option" + (option.selected ? " is-selected" : "");
                        item.setAttribute("role", "option");
                        item.setAttribute("aria-selected", option.selected ? "true" : "false");
                        item.textContent = option.text;
                        item.dataset.value = option.value;
                        item.addEventListener("click", function () {
                            var prevValue;
                            if (!nativeSelect) return;
                            prevValue = nativeSelect.value;
                            if (nativeSelect.value !== this.dataset.value) nativeSelect.value = this.dataset.value;
                            updateText();
                            renderMenu();
                            closeMenu();

                            if (nativeSelect.value === prevValue) return;

                            if (submitOnPick) {
                                if (window.gmSubmitFormWithLoader) {
                                    window.gmSubmitFormWithLoader(nativeSelect.form, "TECH");
                                } else {
                                    if (window.gmUpdateClientContext) window.gmUpdateClientContext();
                                    if (nativeSelect.form && nativeSelect.form.submit) nativeSelect.form.submit();
                                }
                            } else {
                                dispatchNativeChange();
                            }
                        });
                        optionMenu.appendChild(item);
                    }
                }

                function openMenu() {
                    if (!wrapper || !triggerBtn) return;
                    renderMenu();
                    wrapper.classList.add("is-open");
                    triggerBtn.setAttribute("aria-expanded", "true");
                }

                function closeMenu() {
                    if (!wrapper || !triggerBtn) return;
                    wrapper.classList.remove("is-open");
                    triggerBtn.setAttribute("aria-expanded", "false");
                }

                function toggleMenu() {
                    if (!wrapper) return;
                    if (wrapper.classList.contains("is-open")) {
                        closeMenu();
                    } else {
                        openMenu();
                    }
                }

                if (!nativeSelect || !displayText || !wrapper || !triggerBtn || !optionMenu) return;

                updateText();
                renderMenu();

                triggerBtn.addEventListener("mousedown", function (e) {
                    e.preventDefault();
                });

                triggerBtn.addEventListener("click", function (e) {
                    e.preventDefault();
                    e.stopPropagation();
                    toggleMenu();
                });

                nativeSelect.addEventListener("change", function () {
                    updateText();
                    renderMenu();
                });

                document.addEventListener("click", function (e) {
                    if (!wrapper.contains(e.target)) closeMenu();
                });

                document.addEventListener("keydown", function (e) {
                    if (e.key === "Escape") closeMenu();
                });
            }

            initMenuSelect({
                selectId: "report",
                displayId: "gm-report-display",
                wrapperId: "gm-report-dropdown",
                triggerId: "gm-report-trigger",
                menuId: "gm-report-menu"
            });

            initMenuSelect({
                selectId: "clinic",
                displayId: "gm-clinic-display",
                wrapperId: "gm-clinic-dropdown",
                triggerId: "gm-clinic-trigger",
                menuId: "gm-clinic-menu"
            });

            initMenuSelect({
                selectId: "gm-filter1-select",
                displayId: "gm-filter1-display",
                wrapperId: "gm-filter1-dropdown",
                triggerId: "gm-filter1-trigger",
                menuId: "gm-filter1-menu",
                submitOnPick: true
            });

            initMenuSelect({
                selectId: "gm-filter2-select",
                displayId: "gm-filter2-display",
                wrapperId: "gm-filter2-dropdown",
                triggerId: "gm-filter2-trigger",
                menuId: "gm-filter2-menu",
                submitOnPick: true
            });

            initMenuSelect({
                selectId: "gm-filter3-select",
                displayId: "gm-filter3-display",
                wrapperId: "gm-filter3-dropdown",
                triggerId: "gm-filter3-trigger",
                menuId: "gm-filter3-menu",
                submitOnPick: true
            });

            function initTableSort(tableId) {
                var table = document.getElementById(tableId);
                var tbody, sortButtons, i;
                var activeCol = -1;
                var activeDir = "asc";

                if (!table || !table.tHead || !table.tBodies || !table.tBodies.length) return;

                tbody = table.tBodies[0];
                sortButtons = table.querySelectorAll(".gm-main-table-sort");
                if (!sortButtons.length) return;

                for (i = 0; i < tbody.rows.length; i++) {
                    tbody.rows[i].dataset.gmOriginalIndex = String(i);
                }

                function parseCellValue(raw, valueType) {
                    var valueText = String(raw || "").replace(/\s+/g, " ").trim();
                    var normalized;

                    if (valueType === "int") {
                        normalized = valueText.replace(/[^0-9-]/g, "");
                        if (normalized === "" || normalized === "-") return 0;
                        return parseInt(normalized, 10) || 0;
                    }

                    if (valueType === "currency" || valueType === "decimal") {
                        normalized = valueText.replace(/[^0-9.-]/g, "");
                        if (normalized === "" || normalized === "-" || normalized === ".") return 0;
                        return parseFloat(normalized) || 0;
                    }

                    return valueText.toLowerCase();
                }

                function refreshSortState(colIndex, direction) {
                    var headerCells = table.tHead.rows[0].cells;
                    var j;

                    for (j = 0; j < headerCells.length; j++) {
                        headerCells[j].setAttribute("aria-sort", "none");
                        if (sortButtons[j]) {
                            sortButtons[j].classList.remove("is-asc");
                            sortButtons[j].classList.remove("is-desc");
                        }
                    }

                    if (colIndex < 0 || colIndex >= headerCells.length) return;
                    headerCells[colIndex].setAttribute("aria-sort", direction === "asc" ? "ascending" : "descending");
                    if (sortButtons[colIndex]) {
                        sortButtons[colIndex].classList.add(direction === "asc" ? "is-asc" : "is-desc");
                    }
                }

                function sortByColumn(colIndex, valueType, direction) {
                    var rows = Array.prototype.slice.call(tbody.rows);
                    var directionFactor = direction === "desc" ? -1 : 1;
                    var k;

                    rows.sort(function (rowA, rowB) {
                        var valueA = parseCellValue(rowA.cells[colIndex].textContent, valueType);
                        var valueB = parseCellValue(rowB.cells[colIndex].textContent, valueType);
                        var fallbackA = parseInt(rowA.dataset.gmOriginalIndex || "0", 10);
                        var fallbackB = parseInt(rowB.dataset.gmOriginalIndex || "0", 10);

                        if (valueA < valueB) return -1 * directionFactor;
                        if (valueA > valueB) return 1 * directionFactor;
                        return fallbackA - fallbackB;
                    });

                    for (k = 0; k < rows.length; k++) {
                        tbody.appendChild(rows[k]);
                    }
                }

                function bindSortButton(buttonEl) {
                    if (!buttonEl || !buttonEl.parentNode) return;
                    buttonEl.addEventListener("click", function () {
                        var colIndex = buttonEl.parentNode.cellIndex;
                        var valueType = buttonEl.dataset.type || "text";
                        var nextDir = "asc";

                        if (activeCol === colIndex && activeDir === "asc") nextDir = "desc";

                        sortByColumn(colIndex, valueType, nextDir);
                        activeCol = colIndex;
                        activeDir = nextDir;
                        refreshSortState(activeCol, activeDir);
                    });
                }

                for (i = 0; i < sortButtons.length; i++) {
                    bindSortButton(sortButtons[i]);
                }

                refreshSortState(-1, "asc");
            }

            initTableSort("gm-report1-table");

            (function preventReport1HeaderSelection() {
                document.addEventListener("mousedown", function (e) {
                    if (!e.target || !e.target.closest) return;
                    if (!e.target.closest(".gm-main-table-sort")) return;
                    if (window.getSelection) window.getSelection().removeAllRanges();
                });
            })();

            function syncExportFormValues() {
                var sourceUser = document.getElementById("topbar_user_filter");
                var sourceReport = document.getElementById("report");
                var sourceClinic = document.getElementById("clinic");
                var sourceStart = document.getElementById("start_date");
                var sourceEnd = document.getElementById("end_date");
                var sourceFilter1 = getCurrentFilterValue(1);
                var sourceFilter2 = getCurrentFilterValue(2);
                var sourceFilter3 = getCurrentFilterValue(3);
                var targetUser = document.getElementById("gm-export-user-filter");
                var targetReport = document.getElementById("gm-export-report");
                var targetClinic = document.getElementById("gm-export-clinic");
                var targetStart = document.getElementById("gm-export-start-date");
                var targetEnd = document.getElementById("gm-export-end-date");
                var targetFilter1 = document.getElementById("gm-export-filter1");
                var targetFilter2 = document.getElementById("gm-export-filter2");
                var targetFilter3 = document.getElementById("gm-export-filter3");

                updateClientContextFields();
                if (targetUser && sourceUser) targetUser.value = sourceUser.value;
                if (targetReport && sourceReport) targetReport.value = sourceReport.value;
                if (targetClinic && sourceClinic) targetClinic.value = sourceClinic.value;
                if (targetStart && sourceStart) targetStart.value = sourceStart.value;
                if (targetEnd && sourceEnd) targetEnd.value = sourceEnd.value;
                if (targetFilter1) targetFilter1.value = sourceFilter1;
                if (targetFilter2) targetFilter2.value = sourceFilter2;
                if (targetFilter3) targetFilter3.value = sourceFilter3;
            }

            (function bindExportButton() {
                var exportBtn = document.getElementById("gm-export-btn");
                if (!exportBtn) return;
                exportBtn.addEventListener("click", function () {
                    syncExportFormValues();
                });
            })();

            function updateScreenPxInfo(isPrintContext) {
                var topbarCtx = document.getElementById("gm-topbar-ctx-css-px");
                var menuCtx = document.getElementById("gm-menu-ctx-css-px");
                var filterCtx = document.getElementById("gm-filter-ctx-css-px");
                var exportCtx = document.getElementById("gm-export-ctx-css-px");
                var configCtx = document.getElementById("gm-config-ctx-css-px");
                var gconfigCtx = document.getElementById("gm-gconfig-ctx-css-px");
                var sqlCtx = document.getElementById("gm-sql-ctx-css-px");
                var filtersSqlCtx = document.getElementById("gm-filters-sql-ctx-css-px");
                var kpisSqlCtx = document.getElementById("gm-kpis-sql-ctx-css-px");
                var graphSqlCtx = document.getElementById("gm-graph-sql-ctx-css-px");
                var viewportW, viewportH;
                var pxText;

                viewportW = Math.max(document.documentElement.clientWidth || 0, window.innerWidth || 0);
                viewportH = Math.max(document.documentElement.clientHeight || 0, window.innerHeight || 0);
                pxText = Math.round(viewportW) + "x" + Math.round(viewportH) + "px";

                replaceContractLine("- CSS:", "- CSS: " + pxText);
                if (topbarCtx) topbarCtx.value = pxText;
                if (menuCtx) menuCtx.value = pxText;
                if (filterCtx) filterCtx.value = pxText;
                if (exportCtx) exportCtx.value = pxText;
                if (configCtx) configCtx.value = pxText;
                if (gconfigCtx) gconfigCtx.value = pxText;
                if (sqlCtx) sqlCtx.value = pxText;
                if (filtersSqlCtx) filtersSqlCtx.value = pxText;
                if (kpisSqlCtx) kpisSqlCtx.value = pxText;
                if (graphSqlCtx) graphSqlCtx.value = pxText;
                syncSupportLinkFromContract();
            }

            function bindPrintPxTracking() {
                var printMql;
                if (window.matchMedia) {
                    printMql = window.matchMedia("print");
                    if (printMql && printMql.addEventListener) {
                        printMql.addEventListener("change", function (e) {
                            updateScreenPxInfo(!!e.matches);
                        });
                    } else if (printMql && printMql.addListener) {
                        printMql.addListener(function (e) {
                            updateScreenPxInfo(!!e.matches);
                        });
                    }
                }

                window.addEventListener("beforeprint", function () {
                    updateScreenPxInfo(true);
                });
                window.addEventListener("afterprint", function () {
                    updateScreenPxInfo(false);
                });
            }

            function syncExportButtonHorizontal() {
                var showBtn = document.querySelector(".menu .gm-field-action .gm-btn");
                var endDateInput = document.getElementById("end_date");
                var kpisStrip = document.querySelector(".kpis-strip");
                var kpisBoxes = document.querySelector(".kpis-boxes");
                var exportWrap = document.querySelector(".kpis-export");
                var exportBtn = exportWrap ? exportWrap.querySelector(".gm-btn") : null;
                var showRect, endRect, exportRect, showWidth, gapTarget, deltaX;

                if (!showBtn || !kpisStrip || !exportWrap || !exportBtn) return;

                kpisStrip.style.transform = "";
                if (kpisBoxes) kpisBoxes.style.width = "";
                exportWrap.style.marginLeft = "";
                exportWrap.style.flexBasis = "";
                exportWrap.style.width = "";
                exportWrap.style.minWidth = "";

                if (window.matchMedia("(max-width: 900px)").matches) return;

                showRect = showBtn.getBoundingClientRect();
                if (endDateInput) endRect = endDateInput.getBoundingClientRect();
                showWidth = Math.round(showRect.width);
                gapTarget = 0;
                if (endRect) gapTarget = Math.round(showRect.left - endRect.right);
                if (gapTarget < 0) gapTarget = 0;

                exportWrap.style.marginLeft = gapTarget + "px";
                exportWrap.style.flexBasis = showWidth + "px";
                exportWrap.style.width = showWidth + "px";
                exportWrap.style.minWidth = showWidth + "px";

                exportRect = exportBtn.getBoundingClientRect();
                deltaX = Math.round(showRect.left - exportRect.left);
                kpisStrip.style.transform = "translateX(" + deltaX + "px)";
            }

            function scheduleExportAlignment() {
                if (window.requestAnimationFrame) {
                    window.requestAnimationFrame(function () {
                        syncExportButtonHorizontal();
                    });
                } else {
                    setTimeout(function () {
                        syncExportButtonHorizontal();
                    }, 0);
                }
            }

            scheduleExportAlignment();
            setTimeout(scheduleExportAlignment, 120);
            setTimeout(scheduleExportAlignment, 500);
            window.addEventListener("load", scheduleExportAlignment);
            window.addEventListener("resize", scheduleExportAlignment);
            window.addEventListener("load", function () {
                updateScreenPxInfo(false);
            });
            window.addEventListener("resize", function () {
                updateScreenPxInfo(false);
                gmRefreshConfigSticky();
            });
            if (configModalContent && String(configModalContent.textContent || "").trim().length > 0) {
                gmRenderConfigTable(configModalContent.textContent || "");
            }
            gmRenderMainGraph();
            localizeContractDateTimes();
            updateClientContextFields();
            bindPrintPxTracking();
            updateScreenPxInfo(false);
            if (configModal && configModal.classList.contains("is-open")) openConfigModal();
            if (sqlModal && sqlModal.classList.contains("is-open")) openSqlModal();
            if (window.GM_LOADER_DONE) window.GM_LOADER_DONE();
        })();
    </script>
<%
If Not rsData Is Nothing Then
    If rsData.State = 1 Then rsData.Close
    Set rsData = Nothing
End If
If Not rsKPIs Is Nothing Then
    If rsKPIs.State = 1 Then rsKPIs.Close
    Set rsKPIs = Nothing
End If
If Not rsGraph Is Nothing Then
    If rsGraph.State = 1 Then rsGraph.Close
    Set rsGraph = Nothing
End If
%>
</body>
</html>
