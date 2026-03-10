; BLite Server — Windows Installer (Inno Setup 6)
; Copyright (C) 2026 Luca Fabbri — AGPL-3.0
;
; Build with:
;   iscc /DSourceDir=<path-to-publish-dir> blite-server.iss
;
; The CI pipeline calls this after "dotnet publish -r win-x64 --self-contained".

#ifndef SourceDir
  #define SourceDir "..\..\artifacts\win-x64"
#endif
#ifndef AppVersion
  #define AppVersion "1.0.0"
#endif

#define AppName      "BLite Server"
#define AppPublisher "EntglDb"
#define AppURL       "https://github.com/EntglDb/BLite.Server"
#define AppExeName   "BLite.Server.exe"
#define ServiceName  "BLiteServer"
#define ServiceDesc  "BLite Server - self-hosted database (gRPC + REST + Studio)"

[Setup]
AppId={{A3F2C1D4-8E57-4B9A-BC34-21F6E87D5C90}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}/releases
DefaultDirName={autopf}\BLite Server
DefaultGroupName={#AppName}
AllowNoIcons=yes
LicenseFile=..\..\LICENSE
OutputDir=Output
OutputBaseFilename=blite-server-{#AppVersion}-win-x64-setup
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
ArchitecturesInstallIn64BitMode=x64compatible

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "startservice"; Description: "Start BLite Server service after installation"; GroupDescription: "Service:"

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\BLite Server"; Filename: "{app}\{#AppExeName}"
Name: "{group}\{cm:UninstallProgram,{#AppName}}"; Filename: "{uninstallexe}"

; ── Custom configuration pages ────────────────────────────────────────────────

[Code]
var
  ConfigPage:  TInputQueryWizardPage;
  GrpcPort:    String;
  RestPort:    String;
  StudioPort:  String;
  RootKey:     String;
  SourceUrl:   String;
  StudioCheck: TNewCheckBox;

procedure InitializeWizard;
var
  StudioLabel: TLabel;
begin
  ConfigPage := CreateInputQueryPage(
    wpSelectDir,
    'BLite Server Configuration',
    'Configure ports, root API key, and AGPLv3 source URL.',
    'These settings are written to appsettings.Production.json inside the install folder.' + #13#10 +
    'You can edit that file at any time and then restart the service.');

  ConfigPage.Add('gRPC port (default 2626):', False);
  ConfigPage.Add('REST API port (default 2627):', False);
  ConfigPage.Add('Studio (Blazor) port (default 2628):', False);
  ConfigPage.Add('Root API key (min 16 characters):', True);   { password field }
  ConfigPage.Add('Source URL — AGPLv3 §13 compliance:', False);

  ConfigPage.Values[0] := '2626';
  ConfigPage.Values[1] := '2627';
  ConfigPage.Values[2] := '2628';
  ConfigPage.Values[3] := '';
  ConfigPage.Values[4] := 'https://github.com/EntglDb/BLite.Server';

  { "Enable Studio" checkbox below the last input field }
  StudioLabel           := TLabel.Create(ConfigPage.Surface);
  StudioLabel.Parent    := ConfigPage.Surface;
  StudioLabel.Caption   := 'Enable Blazor Studio UI:';
  StudioLabel.Top       := ConfigPage.Edits[4].Top + ConfigPage.Edits[4].Height + 12;
  StudioLabel.Left      := ConfigPage.Edits[4].Left;
  StudioLabel.AutoSize  := True;

  StudioCheck           := TNewCheckBox.Create(ConfigPage.Surface);
  StudioCheck.Parent    := ConfigPage.Surface;
  StudioCheck.Caption   := 'Enable';
  StudioCheck.Checked   := True;
  StudioCheck.Top       := StudioLabel.Top;
  StudioCheck.Left      := StudioLabel.Left + StudioLabel.Width + 8;
  StudioCheck.Width     := 80;
end;

{ Validate input before allowing Next }
function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;

  if CurPageID = ConfigPage.ID then
  begin
    GrpcPort  := Trim(ConfigPage.Values[0]);
    RestPort  := Trim(ConfigPage.Values[1]);
    StudioPort := Trim(ConfigPage.Values[2]);
    RootKey   := Trim(ConfigPage.Values[3]);
    SourceUrl := Trim(ConfigPage.Values[4]);

    if (StrToIntDef(GrpcPort, 0)  < 1) or (StrToIntDef(GrpcPort, 0)  > 65535) then
    begin
      MsgBox('gRPC port must be a valid port number (1–65535).', mbError, MB_OK);
      Result := False; Exit;
    end;
    if (StrToIntDef(RestPort, 0)  < 1) or (StrToIntDef(RestPort, 0)  > 65535) then
    begin
      MsgBox('REST port must be a valid port number (1–65535).', mbError, MB_OK);
      Result := False; Exit;
    end;
    if (StrToIntDef(StudioPort, 0) < 1) or (StrToIntDef(StudioPort, 0) > 65535) then
    begin
      MsgBox('Studio port must be a valid port number (1–65535).', mbError, MB_OK);
      Result := False; Exit;
    end;
    if Length(RootKey) < 16 then
    begin
      MsgBox('Root API key must be at least 16 characters.', mbError, MB_OK);
      Result := False; Exit;
    end;
    if Length(SourceUrl) = 0 then
    begin
      MsgBox('Source URL is required for AGPLv3 §13 compliance.', mbError, MB_OK);
      Result := False; Exit;
    end;
  end;
end;

{ Escape a string for embedding inside a JSON value }
function JsonEscape(const S: String): String;
var
  I:  Integer;
  Ch: Char;
  R:  String;
begin
  R := '';
  for I := 1 to Length(S) do
  begin
    Ch := S[I];
    if Ch = '"'  then R := R + '\"'
    else if Ch = '\' then R := R + '\\'
    else R := R + Ch;
  end;
  Result := R;
end;

{ Write appsettings.Production.json after all files are copied }
procedure WriteAppSettings;
var
  Path:    String;
  Content: String;
  Studio:  String;
begin
  Path := ExpandConstant('{app}\appsettings.Production.json');

  if StudioCheck.Checked then Studio := 'true' else Studio := 'false';

  Content :=
    '{' + #13#10 +
    '  "Auth": {' + #13#10 +
    '    "RootKey": "' + JsonEscape(RootKey) + '"' + #13#10 +
    '  },' + #13#10 +
    '  "BLiteServer": {' + #13#10 +
    '    "DatabasePath": "data\\blite.db",' + #13#10 +
    '    "DatabasesDirectory": "data\\tenants"' + #13#10 +
    '  },' + #13#10 +
    '  "Kestrel": {' + #13#10 +
    '    "Endpoints": {' + #13#10 +
    '      "Grpc": {' + #13#10 +
    '        "Url": "http://*:' + GrpcPort + '",' + #13#10 +
    '        "Protocols": "Http2"' + #13#10 +
    '      },' + #13#10 +
    '      "Rest": {' + #13#10 +
    '        "Url": "http://*:' + RestPort + '",' + #13#10 +
    '        "Protocols": "Http1AndHttp2"' + #13#10 +
    '      },' + #13#10 +
    '      "Studio": {' + #13#10 +
    '        "Url": "http://*:' + StudioPort + '",' + #13#10 +
    '        "Protocols": "Http1AndHttp2"' + #13#10 +
    '      }' + #13#10 +
    '    }' + #13#10 +
    '  },' + #13#10 +
    '  "Studio": {' + #13#10 +
    '    "Enabled": ' + Studio + #13#10 +
    '  },' + #13#10 +
    '  "License": {' + #13#10 +
    '    "SourceUrl": "' + JsonEscape(SourceUrl) + '"' + #13#10 +
    '  }' + #13#10 +
    '}' + #13#10;

  SaveStringToFile(Path, Content, False);
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ResultCode: Integer;
begin
  if CurStep = ssPostInstall then
  begin
    { 1. Write production configuration }
    WriteAppSettings;

    { 2. Create the data subdirectory }
    ForceDirectories(ExpandConstant('{app}\data\tenants'));

    { 3. Register the Windows Service }
    Exec(ExpandConstant('{sys}\sc.exe'),
      'create ' + '{#ServiceName}' +
      ' binPath= "' + ExpandConstant('{app}\{#AppExeName}') + '"' +
      ' start= auto' +
      ' DisplayName= "' + '{#AppName}' + '"',
      '', SW_HIDE, ewWaitUntilTerminated, ResultCode);

    Exec(ExpandConstant('{sys}\sc.exe'),
      'description ' + '{#ServiceName}' +
      ' "' + '{#ServiceDesc}' + '"',
      '', SW_HIDE, ewWaitUntilTerminated, ResultCode);

    { 4. Optionally start the service }
    if WizardIsTaskSelected('startservice') then
      Exec(ExpandConstant('{sys}\sc.exe'),
        'start ' + '{#ServiceName}',
        '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  ResultCode: Integer;
begin
  if CurUninstallStep = usUninstall then
  begin
    { Stop and delete the service before files are removed }
    Exec(ExpandConstant('{sys}\sc.exe'),
      'stop ' + '{#ServiceName}',
      '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
    Exec(ExpandConstant('{sys}\sc.exe'),
      'delete ' + '{#ServiceName}',
      '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end;
end;
