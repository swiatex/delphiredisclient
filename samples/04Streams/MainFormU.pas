unit MainFormU;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.ExtCtrls, Vcl.StdCtrls,
  Vcl.AppEvnts, System.Threading, LoggerPro.GlobalLogger, Redis.Commons, Redis.Client, Redis.Values, Redis.NetLib.Indy;

type
  TMainForm = class(TForm)
    Memo1: TMemo;
    Panel1: TPanel;
    Splitter1: TSplitter;
    btnConn: TButton;
    ApplicationEvents1: TApplicationEvents;
    pnlToolbar: TPanel;
    btnSubscription: TButton;
    btnXADD: TButton;
    btnXRANGE: TButton;
    btnAnotherMe: TButton;
    btnXREAD: TButton;
    btnBulkXADD: TButton;
    Panel2: TPanel;
    Button1: TButton;
    Button2: TButton;
    Button3: TButton;
    Button4: TButton;
    Button5: TButton;
    Memo2: TMemo;
    procedure btnConnClick(Sender: TObject);
    procedure ApplicationEvents1Idle(Sender: TObject; var Done: Boolean);
    procedure btnXADDClick(Sender: TObject);
    procedure btnSubscriptionClick(Sender: TObject);
    procedure btnXRANGEClick(Sender: TObject);
    procedure btnAnotherMeClick(Sender: TObject);
    procedure btnXREADClick(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnBulkXADDClick(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure Button2Click(Sender: TObject);
    procedure Button3Click(Sender: TObject);
    procedure Button4Click(Sender: TObject);
    procedure Button5Click(Sender: TObject);
  private
    fRedis: IRedisClient;
    fTaskSubs, fTaskStreamRead, fTaskStreamReadConsumer1,fTaskCheckMatches: ITask;
    fLastXRANGEID, fLastXREADID: String;
    procedure Log(const MSG: String);
    procedure Log2(const MSG: String);

  public
      procedure CheckMatches;
    { Public declarations }
  end;

      procedure NewOrderConsumer1(const lorder,ltype,lqty,lprice:string);
var
  MainForm: TMainForm;

implementation

uses
  Winapi.ShellAPI, UnitMarket, dateutils;

{$R *.dfm}


procedure TMainForm.ApplicationEvents1Idle(Sender: TObject; var Done: Boolean);
begin
  if Assigned(fRedis) then
  begin
    btnConn.Caption := 'Disconnect';
  end
  else
  begin
    btnConn.Caption := 'Connect';
  end;
  pnlToolbar.Visible := Assigned(fRedis);

  if fLastXRANGEID.IsEmpty then
  begin
    btnXRANGE.Caption := 'XRANGE (get all)';
  end
  else
  begin
    btnXRANGE.Caption := 'XRANGE (' + fLastXRANGEID + ')';
  end;

  if fLastXREADID.IsEmpty then
  begin
    btnXREAD.Caption := 'XREAD (get new)';
  end
  else
  begin
    btnXREAD.Caption := 'XREAD ( > ' + fLastXREADID + ')';
  end;

end;

procedure TMainForm.btnAnotherMeClick(Sender: TObject);
begin
  ShellExecute(0, pchar('open'), pchar(ParamStr(0)), nil, nil, SW_SHOW);
end;

procedure TMainForm.btnBulkXADDClick(Sender: TObject);
var
  lCmd: IRedisCommand;
  lRes: TRedisNullable<string>;
  I: Integer;
begin
  // XADD mystream MAXLEN ~ 1000 * ... entry fields here ...
  for I := 1 to 10000 do
  begin
    lCmd := NewRedisCommand('XADD');
    lCmd
      .Add('mystream')
      .Add('MAXLEN')
      .Add('~')
      .Add(200000)
      .Add('*')
      .Add('key' + I.ToString)
      .Add(Format('Value %4.4d',[I]));
    lRes := fRedis.ExecuteWithStringResult(lCmd);
  end;
//  Log(lRes.Value);
end;

procedure TMainForm.btnConnClick(Sender: TObject);
begin
FormatSettings.DecimalSeparator := '.';

  if Assigned(fRedis) then
  begin
    fRedis.Disconnect;
    fRedis := nil;
  end
  else
  begin
    fRedis := NewRedisClient();
  end;

end;

procedure TMainForm.btnSubscriptionClick(Sender: TObject);
begin
  fTaskSubs := TTask.Run(
    procedure
    begin
      var lRedis := NewRedisClient();
      var lLastID := '';
      while TTask.CurrentTask.Status <> TTaskStatus.Canceled do
      begin
        var lCmd := NewRedisCommand('XREAD');
        lCmd.Add('BLOCK');
        lCmd.Add('1000');
        lCmd.Add('STREAMS');
        lCmd.Add('mystream');

        // XRANGE key start end [COUNT count]
        if lLastID.IsEmpty then
        begin
          lCmd.Add('$');
        end
        else
        begin
          lCmd.Add(lLastID);
        end;
        var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
        if not Assigned(lRes) then
        begin
          Log('Timeout');
          Continue;
        end;
        try
          Log(lRes.ToJSON());
          if lRes.Count > 0 then
          begin
            var
            lSizeOfMyStreamArray := lRes
              .Items[0].ArrayValue
              .Items[1].ArrayValue
              .Count;

            lLastID := lRes
              .Items[0].ArrayValue
              .Items[1].ArrayValue
              .Items[lSizeOfMyStreamArray - 1].ArrayValue
              .Items[0].Value;
          end;
        finally
          lRes.Free;
        end;
      end;
      lRedis.Disconnect();
    end);
end;

procedure TMainForm.btnXADDClick(Sender: TObject);
var
  lCmd: IRedisCommand;
  lRes: TRedisNullable<string>;
begin
  // XADD mystream MAXLEN ~ 1000 * ... entry fields here ...
  lCmd := NewRedisCommand('XADD');
  lCmd
    .Add('mystream')
    .Add('MAXLEN')
    .Add('~')
    .Add(10)
    .Add('*')
    .Add('key1')
    .Add('001' + DateTimeToStr(now))
    .Add('key2')
    .Add('002' + DateTimeToStr(now));
  lRes := fRedis.ExecuteWithStringResult(lCmd);
  Log(lRes.Value);
end;

procedure TMainForm.btnXRANGEClick(Sender: TObject);
begin
  var lCmd := NewRedisCommand('XRANGE');
  lCmd.Add('mystream');

  // XRANGE key start end [COUNT count]
  if fLastXRANGEID.IsEmpty then
  begin
    lCmd.Add('-').Add('+');
  end
  else
  begin
    var
    lPieces := fLastXRANGEID.Split(['-']);
    lCmd.Add(lPieces[0] + '-' + (lPieces[1].ToInteger + 1).ToString).Add('+');
  end;
  var lRes: TRedisRESPArray := fRedis.ExecuteAndGetRESPArray(lCmd);
  try
    Log(lRes.ToJSON());
    if lRes.Count > 0 then
    begin
      fLastXRANGEID := lRes.Items[lRes.Count - 1].ArrayValue.Items[0].Value;
    end;
  finally
    lRes.Free;
  end;
end;

procedure TMainForm.btnXREADClick(Sender: TObject);
begin
  var lCmd := NewRedisCommand('XREAD');
  lCmd.Add('BLOCK');
  lCmd.Add('5000');
  lCmd.Add('STREAMS');
  lCmd.Add('mystream');

  // XRANGE key start end [COUNT count]
  if fLastXREADID.IsEmpty then
  begin
    lCmd.Add('$');
  end
  else
  begin
    lCmd.Add(fLastXREADID);
  end;
  var lRes: TRedisRESPArray := fRedis.ExecuteAndGetRESPArray(lCmd);
  if not Assigned(lRes) then
  begin
    Log('Timeout');
    Exit;
  end;
  try
    Log(lRes.ToJSON());
    if lRes.Count > 0 then
    begin
      var lSizeOfMyStreamArray := lRes
        .Items[0].ArrayValue
        .Items[1].ArrayValue
        .Count;

      fLastXREADID := lRes
        .Items[0].ArrayValue
        .Items[1].ArrayValue
        .Items[lSizeOfMyStreamArray-1].ArrayValue
        .Items[0].Value;
    end;
  finally
    lRes.Free;
  end;
end;

procedure TMainForm.Button1Click(Sender: TObject);
var
  lCmd: IRedisCommand;
  lRes: TRedisNullable<string>;
begin
  // XADD mystream MAXLEN ~ 1000 * ... entry fields here ...
  // XADD order_history:XH2USD * user_id 456 order_type buy quantity 5 price 40000
  lCmd := NewRedisCommand('XADD');
  lCmd
    .Add('order_history:XH2USD')
//    .Add('MAXLEN')
//    .Add('~')
//    .Add(10)
    .Add('*')
    .Add('user_id')
    .Add('456')
    .Add('type')
    .Add('buy')
    .Add('qty')
    .Add(Random(100))
    .Add('price')
    .Add(92+Random(100)/10);
  lRes := fRedis.ExecuteWithStringResult(lCmd);
  Log(lRes.Value);
end;

procedure TMainForm.Button2Click(Sender: TObject);
var
  lCmd: IRedisCommand;
  lRes: TRedisNullable<string>;
begin
  // XADD mystream MAXLEN ~ 1000 * ... entry fields here ...
  // XADD order_history:XH2USD * user_id 456 order_type buy quantity 5 price 40000
  lCmd := NewRedisCommand('XADD');
  lCmd
    .Add('order_history:XH2USD')
//    .Add('MAXLEN')
//    .Add('~')
//    .Add(10)
    .Add('*')
    .Add('user_id')
    .Add('456')
    .Add('type')
    .Add('sell')
    .Add('qty')
    .Add(Random(100))
    .Add('price')
    .Add(100+Random(100)/10);
  lRes := fRedis.ExecuteWithStringResult(lCmd);
  Log(lRes.Value);
end;

procedure TMainForm.Button3Click(Sender: TObject);
begin
  fTaskStreamRead := TTask.Run(
    procedure
    begin
      var lRedis := NewRedisClient();
      var lLastID := '';
      while TTask.CurrentTask.Status <> TTaskStatus.Canceled do
      begin
        var lCmd := NewRedisCommand('XREAD');
        lCmd.Add('BLOCK');
        lCmd.Add('10000');
        lCmd.Add('STREAMS');
        lCmd.Add('order_history:XH2USD');

        // XRANGE key start end [COUNT count]
        if lLastID.IsEmpty then
        begin
          lCmd.Add('$');
        end
        else
        begin
          lCmd.Add(lLastID);
        end;
        var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
        if not Assigned(lRes) then
        begin
          Log('Timeout');
          Continue;
        end;
        try
          Log(lRes.ToJSON());
          if lRes.Count > 0 then
          begin
            var
            lSizeOfMyStreamArray := lRes
              .Items[0].ArrayValue
              .Items[1].ArrayValue
              .Count;

            lLastID := lRes
              .Items[0].ArrayValue
              .Items[1].ArrayValue
              .Items[lSizeOfMyStreamArray - 1].ArrayValue
              .Items[0].Value;
          end;
        finally
          lRes.Free;
        end;
      end;
      lRedis.Disconnect();
    end);
end;

procedure NewOrderConsumer1(const lorder,ltype,lqty,lprice:string);
 function OrderType:string;
 begin
   if uppercase(ltype) = 'BUY' then result := 'BIDS'  else result := 'OFFERS';
 end;
var
  lCmd: IRedisCommand;
  lRes: TRedisNullable<string>;
begin
  // ZADD BIDS:XH2USD 40000 1705351179675-0
  lCmd := NewRedisCommand('ZADD');
  lCmd.Add(Format('%s:XH2USD',[OrderType])).Add(lprice).Add(LOrder);

  lRes := mainform.fRedis.ExecuteWithStringResult(lCmd);
//  mainform.Log(lRes.Value);

  // HSET 1705351179675-0 BUY 40000 101.3
  mainform.fRedis.HMSET(LOrder,['type','qty','price'],[ltype,lqty,lprice]);

end;

procedure TMainForm.Button4Click(Sender: TObject);
begin
  fTaskStreamReadConsumer1 := TTask.Run(
    procedure
    begin
      var lRedis := NewRedisClient();
      var lLastID := '';
      while TTask.CurrentTask.Status <> TTaskStatus.Canceled do
      begin
//        CheckMatches;
        var lCmd := NewRedisCommand('XREAD');
        lCmd.Add('BLOCK');
        lCmd.Add('1000');
        lCmd.Add('STREAMS');
        lCmd.Add('order_history:XH2USD');

        // XRANGE key start end [COUNT count]
        if lLastID.IsEmpty then
        begin
          lCmd.Add('$');
        end
        else
        begin
          lCmd.Add(lLastID);
        end;
        var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
        if not Assigned(lRes) then
        begin
          Log2('Timeout '+DateTimeToUnix(Now()).ToString);
          Continue;
        end;
        try
          Log(lRes.ToJSON());
          if lRes.Count > 0 then
          begin
            var
            lSizeOfMyStreamArray := lRes
              .Items[0].ArrayValue
              .Items[1].ArrayValue
              .Count;

            lLastID := lRes
              .Items[0].ArrayValue
              .Items[1].ArrayValue
              .Items[lSizeOfMyStreamArray - 1].ArrayValue
              .Items[0].Value;

//            lRes.ToJSON()
            var lorder := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[lSizeOfMyStreamArray - 1].ArrayValue.Items[0].Value;
            var ltype := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[lSizeOfMyStreamArray - 1].ArrayValue.Items[1].ArrayValue[3].Value;
            var lqty := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[lSizeOfMyStreamArray - 1].ArrayValue.Items[1].ArrayValue[5].Value;
            var lprice := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[lSizeOfMyStreamArray - 1].ArrayValue.Items[1].ArrayValue[7].Value;
//            log(ltype); log(lqty); log(lprice);
            NewOrderConsumer1(lorder,ltype,lqty,lprice);
            CheckMatches;
          end;
        finally
          lRes.Free;
        end;
      end;
      lRedis.Disconnect();
    end
);
end;

procedure TMainForm.Button5Click(Sender: TObject);
begin
CheckMatches;
end;

procedure TMainForm.CheckMatches;
begin
 MatchOrders;
end;

procedure TMainForm.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(fTaskSubs) then
  begin
    fTaskSubs.Cancel;
    fTaskSubs := nil;
  end;
  if Assigned(fTaskStreamRead) then
  begin
    fTaskStreamRead.Cancel;
    fTaskStreamRead := nil;
  end;
  if Assigned(fTaskStreamReadConsumer1) then
  begin
    fTaskStreamReadConsumer1.Cancel;
    fTaskStreamReadConsumer1 := nil;
  end;
  if Assigned(fTaskCheckMatches) then
  begin
    fTaskCheckMatches.Cancel;
    fTaskCheckMatches := nil;
  end;
end;

procedure TMainForm.Log(const MSG: String);
var
  lValue: String;
begin
  lValue := MSG;
  var lThreadID := TThread.CurrentThread.ThreadID;
  TThread.Queue(nil,
    procedure
    begin
      if Assigned(Memo1) then
      begin
        Memo1.Lines.Add(Format('[TID %d] %s', [lThreadID, lValue]));
      end;
    end);
end;

procedure TMainForm.Log2(const MSG: String);
var
  lValue: String;
begin
  lValue := MSG;
  var lThreadID := TThread.CurrentThread.ThreadID;
  TThread.Queue(nil,
    procedure
    begin
      if Assigned(Memo1) then
      begin
        Memo2.Lines.Add(Format('[TID %d] %s', [lThreadID, lValue]));
      end;
    end);
end;

end.
