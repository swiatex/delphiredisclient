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
    Memo2: TMemo;
    Button5: TButton;
    Button6: TButton;
    Memo3: TMemo;
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
    procedure Button6Click(Sender: TObject);
  private

    fTaskSubs, fTaskStreamRead, fTaskStreamReadConsumer1,fTaskCheckMatches: ITask;
    fLastXRANGEID, fLastXREADID: String;
    procedure Log(const MSG: String);
    procedure Log3(const MSG: String);
    procedure Log2(const MSG: String);

  public
      fRedis: IRedisClient;
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

 function OrderType(ltype:string):string;
 begin
   if uppercase(ltype) = 'BUY' then result := 'BIDS'  else result := 'OFFERS';
 end;

procedure NewOrderConsumer1(const lorder,ltype,lqty,lprice:string);
var
  lCmd: IRedisCommand;
  lRes: TRedisNullable<string>;
begin
 var RedisClient := NewRedisClient();

  RedisClient.ZADD(Format('%s:XH2USD',[OrderType(ltype)]),lprice.ToDouble,LOrder);
  RedisClient.HMSET(LOrder,['type','qty','price'],[ltype,lqty,lprice]);

  RedisClient.Disconnect;
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
       try
        var lCmd := NewRedisCommand('XREAD').Add('BLOCK').Add('5000').Add('STREAMS').Add('order_history:XH2USD');

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
            var lSizeOfMyStreamArray := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Count;

            var i:integer;
            for i := 0 to lSizeOfMyStreamArray-1 do begin

            lLastID := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[i].ArrayValue.Items[0].Value;

//            lRes.ToJSON()
            var lorder := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[i].ArrayValue.Items[0].Value;
            var ltype  := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[i].ArrayValue.Items[1].ArrayValue[3].Value;
            var lqty   := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[i].ArrayValue.Items[1].ArrayValue[5].Value;
            var lprice := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[i].ArrayValue.Items[1].ArrayValue[7].Value;

            lRedis.ZADD(Format('%s:XH2USD',[OrderType(ltype)]),lprice.ToDouble,LOrder);
            lRedis.HMSET(LOrder,['type','qty','price'],[ltype,lqty,lprice]);

            MatchOrders(ltype);
            end;
          end;
        finally
          lRes.Free;
        end;
         except
         on E : Exception do begin {showmessage(E.ToString);}    log3(E.ToString); end;
       end;
      end;
      lRedis.Disconnect();
    end
);

end;



procedure TMainForm.Button5Click(Sender: TObject);
begin
  // XADD mystream MAXLEN ~ 1000 * ... entry fields here ...
  // XADD order_history:XH2USD * user_id 456 order_type buy quantity 5 price 40000

 TTask.Run( procedure var i:integer; begin
  for I := 1 to 10000 do begin
   var lCmd := NewRedisCommand('XADD');
   lCmd
    .Add('order_history:XH2USD')
    .Add('*')
    .Add('user_id')
    .Add('456')
    .Add('type')
    .Add('sell')
    .Add('qty')
    .Add(Random(100)+1)
    .Add('price')
    .Add(100+Random(100)/10);
   var lRes := fRedis.ExecuteWithStringResult(lCmd);
  if i mod 100 = 0 then Log(i.ToString);
     sleep(5);
  end;
 end
  );
end;

procedure TMainForm.Button6Click(Sender: TObject);
begin
TTask.Run( procedure var i:integer; begin
  for I := 1 to 10000 do begin
    var lCmd := NewRedisCommand('XADD');
  lCmd
    .Add('order_history:XH2USD')
    .Add('*')
    .Add('user_id')
    .Add('456')
    .Add('type')
    .Add('buy')
    .Add('qty')
    .Add(Random(100)+1)
    .Add('price')
    .Add(92+Random(100)/10);
  var lRes := fRedis.ExecuteWithStringResult(lCmd);
  if i mod 100 = 0 then Log(i.ToString);
  sleep(5);
  end;
  end);
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

procedure TMainForm.Log3(const MSG: String);
var
  lValue: String;
begin
  lValue := MSG;
  var lThreadID := TThread.CurrentThread.ThreadID;
  TThread.Queue(nil,
    procedure
    begin
      if Assigned(Memo3) then
      begin
        Memo3.Lines.Add(Format('[TID %d] %s', [lThreadID, lValue]));
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
      if Assigned(Memo2) then
      begin
        Memo2.Lines.Add(Format('[TID %d] %s', [lThreadID, lValue]));
      end;
    end);
end;

end.
