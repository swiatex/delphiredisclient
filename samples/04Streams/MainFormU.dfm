object MainForm: TMainForm
  Left = 0
  Top = 0
  Caption = 'REDIS Streams Sample'
  ClientHeight = 445
  ClientWidth = 885
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OnClose = FormClose
  TextHeight = 13
  object Splitter1: TSplitter
    Left = 0
    Top = 65
    Width = 885
    Height = 5
    Cursor = crVSplit
    Align = alTop
    ExplicitTop = 85
    ExplicitWidth = 724
  end
  object Memo1: TMemo
    Left = 0
    Top = 70
    Width = 687
    Height = 272
    Align = alClient
    Font.Charset = ANSI_CHARSET
    Font.Color = clWindowText
    Font.Height = -16
    Font.Name = 'Courier New'
    Font.Style = []
    ParentFont = False
    ScrollBars = ssBoth
    TabOrder = 0
    WordWrap = False
  end
  object Panel1: TPanel
    Left = 0
    Top = 0
    Width = 885
    Height = 65
    Align = alTop
    TabOrder = 1
    object btnConn: TButton
      AlignWithMargins = True
      Left = 4
      Top = 4
      Width = 105
      Height = 57
      Align = alLeft
      Caption = 'Button1'
      TabOrder = 0
      OnClick = btnConnClick
    end
    object pnlToolbar: TPanel
      Left = 112
      Top = 1
      Width = 772
      Height = 63
      Align = alClient
      BevelOuter = bvLowered
      Caption = 'pnlToolbar'
      TabOrder = 1
      object btnSubscription: TButton
        AlignWithMargins = True
        Left = 4
        Top = 4
        Width = 161
        Height = 55
        Align = alLeft
        Caption = 'Subscribe'
        TabOrder = 0
        WordWrap = True
        OnClick = btnSubscriptionClick
      end
      object btnXADD: TButton
        AlignWithMargins = True
        Left = 260
        Top = 4
        Width = 83
        Height = 55
        Align = alLeft
        Caption = 'XADD'
        TabOrder = 1
        OnClick = btnXADDClick
      end
      object btnXRANGE: TButton
        AlignWithMargins = True
        Left = 349
        Top = 4
        Width = 83
        Height = 55
        Align = alLeft
        Caption = 'XRANGE (get all)'
        TabOrder = 2
        WordWrap = True
        OnClick = btnXRANGEClick
      end
      object btnAnotherMe: TButton
        Left = 712
        Top = 1
        Width = 59
        Height = 61
        Align = alRight
        Caption = 'New Instance'
        TabOrder = 3
        WordWrap = True
        OnClick = btnAnotherMeClick
      end
      object btnXREAD: TButton
        AlignWithMargins = True
        Left = 438
        Top = 4
        Width = 83
        Height = 55
        Align = alLeft
        Caption = 'XREAD (get new)'
        TabOrder = 4
        WordWrap = True
        OnClick = btnXREADClick
      end
      object btnBulkXADD: TButton
        AlignWithMargins = True
        Left = 171
        Top = 4
        Width = 83
        Height = 55
        Align = alLeft
        Caption = 'XADD (bulk)'
        TabOrder = 5
        OnClick = btnBulkXADDClick
      end
    end
  end
  object Panel2: TPanel
    Left = 0
    Top = 406
    Width = 885
    Height = 39
    Align = alBottom
    TabOrder = 2
    object Button1: TButton
      AlignWithMargins = True
      Left = 311
      Top = 4
      Width = 82
      Height = 31
      Align = alLeft
      Caption = '1x BUY'
      TabOrder = 0
      OnClick = Button1Click
    end
    object Button2: TButton
      AlignWithMargins = True
      Left = 214
      Top = 4
      Width = 91
      Height = 31
      Align = alLeft
      Caption = '1x SELL'
      TabOrder = 1
      OnClick = Button2Click
    end
    object Button3: TButton
      AlignWithMargins = True
      Left = 115
      Top = 4
      Width = 93
      Height = 31
      Align = alLeft
      Caption = 'Subscribe'
      TabOrder = 2
      WordWrap = True
      OnClick = Button3Click
    end
    object Button4: TButton
      AlignWithMargins = True
      Left = 4
      Top = 4
      Width = 105
      Height = 31
      Align = alLeft
      Caption = 'Sub + Consumer1'
      TabOrder = 3
      WordWrap = True
      OnClick = Button4Click
    end
    object Button5: TButton
      AlignWithMargins = True
      Left = 399
      Top = 4
      Width = 61
      Height = 31
      Align = alLeft
      Caption = 'bulk SELL'
      TabOrder = 4
      OnClick = Button5Click
    end
    object Button6: TButton
      AlignWithMargins = True
      Left = 466
      Top = 4
      Width = 66
      Height = 31
      Align = alLeft
      Caption = 'bulk BUY'
      TabOrder = 5
      OnClick = Button6Click
    end
  end
  object Memo2: TMemo
    Left = 0
    Top = 342
    Width = 885
    Height = 64
    Align = alBottom
    Font.Charset = DEFAULT_CHARSET
    Font.Color = clWindowText
    Font.Height = -8
    Font.Name = 'Tahoma'
    Font.Style = []
    ParentFont = False
    TabOrder = 3
  end
  object Memo3: TMemo
    Left = 687
    Top = 70
    Width = 198
    Height = 272
    Align = alRight
    Font.Charset = DEFAULT_CHARSET
    Font.Color = clWindowText
    Font.Height = -8
    Font.Name = 'Tahoma'
    Font.Style = []
    ParentFont = False
    ScrollBars = ssBoth
    TabOrder = 4
    WordWrap = False
  end
  object ApplicationEvents1: TApplicationEvents
    OnIdle = ApplicationEvents1Idle
    Left = 360
    Top = 192
  end
end
