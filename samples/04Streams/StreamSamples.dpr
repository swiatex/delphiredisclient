program StreamSamples;

uses
  Vcl.Forms,
  MainFormU in 'MainFormU.pas' {MainForm},
  UnitMarket in 'UnitMarket.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TMainForm, MainForm);
  Application.Run;
end.
